import time
from scrapy.spiders import CrawlSpider
import scrapy
import pandas as pd
import os
from bs4 import BeautifulSoup
import requests
import _thread
import json


class MarketwatcherSpider(CrawlSpider):
    name = 'marketwatcher'
    allowed_domains = ['www.marketwatch.com']
    start_urls = ['https://www.marketwatch.com/tools/markets/stocks/a-z/0-9']
    company_link = "https://www.marketwatch.com/investing/stock/"
    data_pages = ["/financials/income", "/financials/balance-sheet", "/financials/cash-flow"]

    data = pd.DataFrame()
    data_counter = 0
    max_speed = 0.19
    throttle = (False, 0)
    errors = []

    def display_info(self):
        self.start_time = time.time()
        while 1:
            os.system('cls')
            print(f"Total data_points collected: {self.data_counter}")
            speed = (self.data_counter / (time.time() - self.start_time + 0.0001))
            print(f"Data_points collected per sec {speed:.2f}")
            print(f"Previous tickers left to check: {len(self.prev_tickers)}")
            time.sleep(0.25)

    def start_requests(self):
        self.prev_tickers = []
        try:
            with open("../data/data.jsonlines", encoding='utf-8') as file:
                for line in file:
                    self.prev_tickers.append(list(json.loads(line).keys())[0])
        except FileNotFoundError as e:
            self.errors.append(e)

        _thread.start_new_thread(self.display_info, ())
        yield scrapy.Request(url=self.start_urls[0], callback=self.get_page)

    def get_page(self, response):
        for idx, page in enumerate(response.css("ul.pagination li a::attr(href)").getall()):
            yield response.follow(url=page, callback=self.download_tickers, cb_kwargs={"page": page, "depth": 0})

    def download_tickers(self, response, page, depth):
        if response.status != 200:
            if depth >= 6:
                return
            time.sleep(60 * depth)
            yield response.follow(url = page, callback=self.download_tickers, cb_kwargs={"page": page, "depth": depth+1})
        else:
            links = response.css('table.table.table-condensed a::attr(href)').getall()

            for idx, link in enumerate(links):
                if self.throttle[0]:
                    time.sleep(self.throttle[1])
                yield response.follow(url=link,
                                      callback=self.get_data)

            pagination = response.css('ul.pagination')[-1]
            if pagination.css('li a::text').getall()[-1].strip() == "»":
                link = pagination.css('li a::attr(href)').getall()[-1]
                yield response.follow(url=link,
                                      callback=self.download_tickers)

    def get_data(self, response):
        intraday_data = response.css("div.region.region--intraday")
        ticker = intraday_data.css("span.company__ticker::text").get().strip()

        if len(self.prev_tickers) > 0:
            if ticker in self.prev_tickers:
                self.prev_tickers.remove(ticker)
                return

        region = intraday_data.css("span.company__market::text").get().strip()
        try:
            currency = intraday_data.css("h2.intraday__price span.character::text").get().strip()
        except Exception as e:
            self.errors.append(e)
            currency = intraday_data.css("h2.intraday__price sup.character::text").get().strip()
        try:
            price = intraday_data.css("h2.intraday__price  span.value::text").get().strip()
        except Exception as e:
            self.errors.append(e)
            price = intraday_data.css("h2.intraday__price  bg-quote.value::text").get().strip()

        data_ = {ticker: {"currency": currency, "region": region, "price": price}}

        html_page = requests.get(
            f'https://www.marketwatch.com/investing/stock/{ticker}/company-profile?mod=mw_quote_tab').content
        soup = BeautifulSoup(html_page, 'html.parser')
        sector_industry = soup.find_all('li', class_="kv__item w100")

        for part in sector_industry:
            for label, text in zip(part.find_all('small', class_="label"), part.find_all('span', class_="primary")):
                data_[label.text] = text.text

        data_frames = []
        try:
            for data_page in self.data_pages:
                data = pd.read_html(self.company_link + ticker + data_page)[4:]

                if type(data) == list:
                    for df in data:
                        data_frames.append(df)
                else:
                    data_frames.append(data)

            combined_data_frame = pd.concat(data_frames, ignore_index=True)
        except Exception as e:
            self.errors.append(e)
            return

        combined_data_frame = combined_data_frame.iloc[:, :-1]
        indexes = [text[:(len(text) // 2) - 1] for text in combined_data_frame.iloc[:, 0]]
        combined_data_frame.drop(columns="Item  Item", inplace=True)
        combined_data_frame.index = indexes

        self.data_counter += 1
        yield {ticker: {"details": combined_data_frame.transpose().to_dict(), "basic": data_}}
