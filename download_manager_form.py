from PyQt5 import QtWidgets, uic
import os
import sys
import json
import connection
import pandas as pd

class download_manager_form(QtWidgets.QMainWindow):
    def __init__(self, username, password, mysql_host, sftp_host, mysql_port, sftp_port, id_sample_list, mode_list, parent):
        QtWidgets.QMainWindow.__init__(self, parent)
        # Setting the position and size of the UI
        self.ui = uic.loadUi(os.path.abspath('source/download_manager_form.ui'), self)
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.sftp_host = sftp_host
        self.sftp_port = sftp_port
        self.mode_list = mode_list
        self.username, self.password = username, password
        self.id_sample_list = id_sample_list
        self.init_function()
        self.error_dialog = QtWidgets.QErrorMessage()
        self.download_progress_bar.setValue(0)

        self.download_message = "Ready to download the data from id_sample: " + ", ".join([str(id) for id in self.id_sample_list]) + "\n"

        self.download_progress_message.setText(self.download_message)
        self.ui.show()

    def mysql_connection_open(self):
        con_MySQL = connection.get_MySQL_Connection(self.mysql_host, self.mysql_port, 'tfdb', self.username, self.password)
        return con_MySQL

    def sftp_connection_open(self):
        con_SFTP = connection.get_SFTP_Connection_paramiko(self.sftp_host, self.sftp_port, self.username, self.password)
        return con_SFTP

    def init_function(self):
        self.group = QtWidgets.QButtonGroup()
        self.group.setExclusive(False)


        for i in range(len(self.mode_list)):
            row = i//5 + 1
            column = i % 5
            widget = QtWidgets.QCheckBox(self.mode_list[i])
            self.group.addButton(widget)
            self.mode_gridLayout.addWidget(widget, row, column)
        self.all_checkbox.setChecked(True)


        with open(os.path.abspath('source/system_config.json'), 'r', encoding='utf-8') as f:
            self.system_config = json.load(f)
        download_directory = self.system_config["download_directory"]
        self.directory_line_edit.setText(download_directory)



    def browse_button_clicked(self):
        directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Directory")
        if directory:
            self.directory_line_edit.setText(directory)
            self.system_config["download_directory"] = directory
            with open(os.path.abspath('source/system_config.json'), "w") as f:
                json.dump(self.system_config, f)

    def all_checkbox_changed(self):
        disable_rest = self.all_checkbox.isChecked()
        for i in range(len(self.mode_list)):
            widget = self.mode_gridLayout.itemAt(i + 1).widget()
            widget.setDisabled(disable_rest)

    def download_data_with_sample_id(self, con_sql, con_sftp, local_directory_path, id_sample, mode_list):
        simple_dict = connection.initial_setting(con_sql, local_directory_path, mode_list)
        id_list_dict = connection.get_existing_property_metadata_id_list(con_sql, id_sample, mode_list)
        fail_list = []
        for mode in mode_list:
            id_list = id_list_dict[mode]
            simple = simple_dict[mode]
            for id_property in id_list:
                self.download_progress_message.setText(self.download_message + f"downloading {id_sample}-{mode}-{id_property} ...")
                success, error_message = connection.download_data_with_sample_id_and_property_id(con_sftp, local_directory_path,
                                                                                      mode,
                                                                                      simple, id_sample, id_property)
                if not success:
                    fail_list.append([f"/TFDB_drive/{mode}/{id_sample}-{id_property}", error_message])
                    self.download_message += f"{id_sample}-{mode}-{id_property}---Download Failed ({error_message})\n"
                else:
                    self.download_message += f"{id_sample}-{mode}-{id_property}---Downloaded \n"
                self.download_progress_message.setText(self.download_message)
        return fail_list

    def download_metadata(self, con_sql, mode_list, download_path):
        mode_list_added_sample = mode_list + ["Sample"]
        where_query = "WHERE " + " OR ".join([f"id_sample = {id}" for id in self.id_sample_list])
        for mode in mode_list_added_sample:
            select_query =  f"SELECT * FROM tfdb.{mode} "
            query = select_query + where_query + ";"
            df = pd.read_sql(select_query + where_query + ";", con=con_sql)
            path = download_path + f"/{mode}_metadata.csv"
            df.to_csv(path)


    def download_button_clicked(self):
        download_path = self.directory_line_edit.text()
        try:
            path_stat = os.stat(download_path)
        except:
            self.error_dialog.showMessage("Path doesn't exist. Please check the directory path exists.")
            return 0



        if self.all_checkbox.isChecked():
            download_mode_list = self.mode_list
        else:
            download_mode_list = []
            for i in range(len(self.mode_list)):
                widget = self.mode_gridLayout.itemAt(i + 1).widget()
                if widget.isChecked():
                    download_mode_list.append(widget.text())
        download_list_count = len(self.id_sample_list)
        with self.mysql_connection_open() as con_sql, self.sftp_connection_open() as con_sftp:
            if self.metadata_checkbox.isChecked():
                print("HEy iI am hereeee")
                self.download_metadata(con_sql, download_mode_list, download_path)

            for i in range(download_list_count):
                id_sample = self.id_sample_list[i]
                self.download_progress_bar.setValue(int(i / download_list_count * 100))
                fail_list = self.download_data_with_sample_id(con_sql, con_sftp, download_path, id_sample, download_mode_list)
        self.download_progress_bar.setValue(100)

def main():

    app = QtWidgets.QApplication(sys.argv)
    sample_id_list = [265,266]
    w = download_manager_form("manager", "Ammd21306!", "localhost", "localhost", 2206, 2202,sample_id_list, parent=None)
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
    """
    SELECT distinct s.id_sample
FROM tfdb.Sample AS s
JOIN `tfdb`.`Project` AS p ON s.id_project = p.id_project
JOIN `tfdb`.`Experimenter` AS e ON s.id_experimenter = e.id_experimenter
;
    """