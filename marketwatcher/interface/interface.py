from tkinter import *
from tkinter import ttk
from os import system
from _thread import start_new_thread

width = 300
height = 200
buttons = []

# root window
window = Tk()
window.geometry(f'{width}x{height}')
window.resizable(False, False)
window.title('Market watcher')

# exit button
exit_button = ttk.Button(
    window,
    text='Exit',
    command=lambda: window.quit()
)
buttons.append(exit_button)

exit_button.pack(
    ipadx=5,
    ipady=5,
    expand=True
)

def companier_button_fun(idx, commands):
    buttons[idx]['state'] = DISABLED
    for command in commands:
        start_new_thread(system, (command, ))

companier_button = ttk.Button(
    window,
    text="Run companier",
    command=lambda idx=1, commands=("cd ..", "scrapy crawl companier -o ../data/companies.jsonlines"): companier_button_fun(idx, commands)
)
buttons.append(companier_button)

companier_button.pack(
    ipadx=5,
    ipady=5,
    expand=True
)


bonder_button = ttk.Button(
    window,
    text="Run bonder",
    command=lambda idx=2, commands=("cd ..", "scrapy crawl bonder -o ../data/bonds.jsonlines"): companier_button_fun(idx, commands)
)
buttons.append(bonder_button)

bonder_button.pack(
    ipadx=5,
    ipady=5,
    expand=True
)

exit_button.place(x=220, y=170)

companier_button.place(x=width/6, y=height/2)

bonder_button.place(x=3*width/6, y=height/2)

window.mainloop()
