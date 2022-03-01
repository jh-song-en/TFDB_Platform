from PyQt5 import QtWidgets, uic, QtGui, QtCore
from PyQt5.QtWidgets import *
from PyQt5.QtCore import pyqtSlot
import os
import sys
import connection
from main_form import main_form
import json

class LoginForm(QtWidgets.QWidget):
    def __init__(self):
        QtWidgets.QWidget.__init__(self, None)
        self.ui = uic.loadUi(os.path.abspath('source/login_form.ui'), self)
        self.setWindowFlags(QtCore.Qt.FramelessWindowHint)

        with open(os.path.abspath('source/system_config.json'), 'r', encoding='utf-8') as f:
            self.system_config = json.load(f)

        self.set_system_config()

        self.error_dialog = QtWidgets.QErrorMessage()
        self.ui.show()

    def set_system_config(self):
        self.mysql_host = self.system_config["MySQL_host"]
        self.mysql_port = self.system_config["MySQL_port"]
        self.sftp_host = self.system_config["SFTP_host"]
        self.sftp_port = self.system_config["SFTP_port"]

    def mysql_connection_open(self):
        con_MySQL = connection.get_MySQL_Connection(self.mysql_host, self.mysql_port, 'tfdb', self.username, self.password)
        return con_MySQL

    def sftp_connection_open(self):
        con_SFTP = connection.get_SFTP_Connection_paramiko(self.sftp_host, self.sftp_port, self.username, self.password)

        return con_SFTP

    @pyqtSlot()
    def login_button_clicked(self):
        self.username = self.username_edit.text()
        self.password = self.password_edit.text()
        try:
            with self.mysql_connection_open() as con_sql, self.sftp_connection_open() as con_sftp:
                self.main_form = main_form(self.username, self.password, self.mysql_host, self.sftp_host, self.mysql_port, self.sftp_port)
            self.system_config["user_id"] = self.username
            with open(os.path.abspath('source/system_config.json'), "w") as f:
                json.dump(self.system_config, f)
            self.close()
        except:
            self.error_dialog.showMessage(str(sys.exc_info()))
            #self.error_dialog.showMessage("Incorrect username or password or connection error")

    def server_setting_button_clicked(self):
        self.login_form_stacked_widget.setCurrentIndex(1)
        self.mysql_host_edit.setText(self.system_config["MySQL_host"])
        self.mysql_port_edit.setText(self.system_config["MySQL_port"])
        self.sftp_host_edit.setText(self.system_config["SFTP_host"])
        self.sftp_port_edit.setText(self.system_config["SFTP_port"])



    def server_setting_cancel_button_clicked(self):
        self.login_form_stacked_widget.setCurrentIndex(0)

    def server_setting_save_button_clicked(self):
        self.system_config["MySQL_host"] = self.mysql_host_edit.text()
        self.system_config["MySQL_port"] =  self.mysql_port_edit.text()
        self.system_config["SFTP_host"] = self.sftp_host_edit.text()
        self.system_config["SFTP_port"] = self.sftp_port_edit.text()
        self.set_system_config()
        with open(os.path.abspath('source/system_config.json'), "w") as f:
            json.dump(self.system_config, f)
        self.login_form_stacked_widget.setCurrentIndex(0)

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)

    sys.exit(app.exec())
