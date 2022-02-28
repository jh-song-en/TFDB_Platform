# -*- coding: utf-8 -*-
"""
Created on Mon Aug 31 09:18:08 2020

@author: Jaeho Song
@author connection: jaeho.song.en@gmail.com
"""
import sys
from connection import get_MySQL_Connection
from connection import get_SFTP_Connection_paramiko
from PyQt5 import QtWidgets
from login_form import LoginForm


def main():

    app = QtWidgets.QApplication(sys.argv)
    w = LoginForm()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
