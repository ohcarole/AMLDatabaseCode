import Tkinter
import tkMessageBox

window = Tkinter.Tk()
window.wm_withdraw()

MsgResp = tkMessageBox.showinfo(title="Log Download", message="Would you like to send email to Center IT?", type="yesno")
window.wm_withdraw()
print(MsgResp)

import ctypes  # An included library with Python install.
# import easygui

def Mbox(title, text, style):
    return ctypes.windll.user32.MessageBoxW(0, text, title, style)
resp = Mbox('Would you like to0', 'Dance?', 4)
print(resp)

"""
Have you looked at easygui?
import easygui
easygui.msgbox("This is a message!", title="simple gui")
"""