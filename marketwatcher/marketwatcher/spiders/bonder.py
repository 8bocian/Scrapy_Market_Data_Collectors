from scrapy.spiders import CrawlSpider
from scrapy import Request


class BondSpider(CrawlSpider):
    name = 'bonder'
    allowed_domains = ['www.marketwatch.com']
    url = 'https://www.marketwatch.com/tools/markets/bonds/a-z'

    def start_requests(self):
        yield Request(url=self.url, callback=self.parse)

    def parse(self, response, **kwargs):
        for link in response.css('ul.pagination li a'):
            link = link.attrib['href']
            yield response.follow(url=link, callback=self.parse_section, cb_kwargs={'link': link})

    def parse_section(self, response, link):
        bond_links = response.css('table.table.table-condensed td.name a')
        for bond_link in bond_links:
            bond_link = bond_link.attrib['href']
            yield response.follow(url=bond_link, callback=self.parse_bond)

    def parse_bond(self, response, **kwargs):
        name = response.css('span.company__ticker::text').get()
        bond = response.css('h1.company__name::text').get()
        rate = response.css('h2.intraday__price.sup--right span.value::text').get()
        if rate is None:
            rate = response.css('h2.intraday__price.sup--right bg-quote.value::text').get()
        if rate is None:
            return
        yield {name: bond,
               'rate': rate}
