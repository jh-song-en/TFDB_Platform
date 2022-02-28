"""


    Result form. shows the data of the sample clicked from Search section.

"""
import os
import sys
from PyQt5 import QtWidgets, uic, QtGui
from PyQt5 import QtWebEngineWidgets
from PyQt5.QtCore import pyqtSlot
from PyQt5.QtWidgets import *
from PyQt5.QtWidgets import QMessageBox
import pandas as pd

import data_plot
import connection
class sub_form(QtWidgets.QMainWindow):
    def __init__(self, username, password, mysql_host, sftp_host, mysql_port, sftp_port, sample_id, parent):
        # UI_setting
        self.sample_id = sample_id
        QtWidgets.QMainWindow.__init__(self, parent)
        self.ui = uic.loadUi(os.path.abspath('source/Sample_form.ui'), self)
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.sftp_host = sftp_host
        self.sftp_port = sftp_port
        # The functions below need to be remained

        self.username, self.password = username, password
        data_plot.show_fig()

        self.authority_setting()

        self.set_sample_meta_grid()

        self.meta_id = ""
        self.mode = ""
        self.sample_metadata = []
        self.error_dialog = QtWidgets.QErrorMessage()
        self.reset_metadata()
        self.ui.show()
        self.current_plot = ""


    def authority_setting(self):
        with self.mysql_connection_open() as con_sql:
            import pandas as pd
            df = pd.read_sql("SHOW GRANTS FOR CURRENT_USER;", con_sql)

            self.delete_authority, self.insert_authority, self.update_authority = connection.authority_check(con_sql)

        self.result_sample_delete_button.setEnabled(self.delete_authority)
        self.result_meta_delete_button.setEnabled(self.delete_authority)

        self.result_sample_edit_button.setEnabled(self.update_authority)
        self.result_meta_edit_button.setEnabled(self.update_authority)

    def mysql_connection_open(self):
        con_MySQL = connection.get_MySQL_Connection(self.mysql_host, self.mysql_port, 'tfdb', self.username, self.password)
        return con_MySQL

    def sftp_connection_open(self):
        con_SFTP = connection.get_SFTP_Connection_paramiko(self.sftp_host, self.sftp_port, self.username, self.password)
        return con_SFTP

    def metadata_category_info_to_df(self, mode):
        query = f"SELECT * FROM tfdb_config.metadata_category WHERE `property` = '{mode}';"
        with self.mysql_connection_open() as con_sql:
            try:
                metadata_category_info_df = pd.read_sql(query, con=con_sql)
                return metadata_category_info_df
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)


    def set_sample_meta_grid(self):
        self.sample_columns_info_df = self.get_current_info_table()
        total_columns = len(self.sample_columns_info_df) - 5
        div = 4
        grid_rows = int(total_columns / div) + (total_columns % div > 0)

        df = self.sample_columns_info_df[4:].reset_index(drop=True)

        for i in reversed(range(self.sample_meta_grid.count())):
            self.sample_meta_grid.itemAt(i).widget().setParent(None)

        for i in range(total_columns):

            display_text = df["display_text"][i]
            row = i % grid_rows
            column = (i // grid_rows) * 2
            self.sample_meta_grid.addWidget(QLabel(display_text), row, column)

            widget = QLineEdit()
            widget.setReadOnly(True)

            self.sample_meta_grid.addWidget(widget, row, column + 1)

    def get_current_info_table(self):

        query = "SELECT * FROM `tfdb_config`.`sample_columns_info`;"
        with self.mysql_connection_open() as con_sql:
            try:
                df = pd.read_sql(query, con=con_sql)
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
                return 0

        return df


    def metadata_columns_info_to_df(self, mode):
        query = f"SELECT `id_metadata` FROM `tfdb_config`.`metadata_category` WHERE `property` = '{mode}';"
        with self.mysql_connection_open() as con_sql:
            try:
                cursor = con_sql.cursor()
                cursor.execute(query)
                temp = cursor.fetchall()
                id_metadata = temp[0][0]
                query = f"SELECT * FROM tfdb_config.metadata_columns_info WHERE id_columns LIKE \'%-{id_metadata}\' " \
                        f"ORDER BY cast(id_columns as unsigned) ASC;"
                metadata_columns_info_df = pd.read_sql(query, con=con_sql)
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
        return metadata_columns_info_df

    def reset_metadata(self):
        self.get_and_show_sample_metadata()
        self.show_property_metadata()

    def show_property_metadata(self):
        with self.mysql_connection_open() as con_sql:
            query = f"SELECT `property` FROM `tfdb_config`.`metadata_category`;"
            cursor = con_sql.cursor()
            cursor.execute(query)
            temp = cursor.fetchall()
            mode_list = [i[0] for i in temp]
            df = connection.advanced_search(con_sql,str(self.sample_id),"ID",mode_list)
            combo_list = []
            for mode in mode_list:
                if df[mode][0] != 0 and df[mode][0] != "":
                    combo_list.append(mode)
            if len(combo_list) == 0:
                combo_list = ["None"]
            self.prop_combo.clear()
            self.prop_combo.addItems(combo_list)


    def mode_meta_change(self):
        """
        This changes the Form Layout dataFormLayout of data upload page
        the contents of Form Layout changes from the
        """
        self.mode = self.prop_combo.currentText()
        if self.mode == "":
            return 0
        if self.mode == "None":
            combo_list = ["None"]
        else:

            self.current_mode_metadata_category_info_df = self.metadata_category_info_to_df(self.mode)
            self.current_mode_metadata_columns_info_df = self.metadata_columns_info_to_df(self.mode)

            self.result_plot_view.setHtml("Loading...")
            with self.mysql_connection_open() as con_sql:
                mode_id_list = connection.get_metadata_id_list(con_sql, self.mode, self.sample_id)
            combo_list = [str(mode_id[0]) for mode_id in mode_id_list]
            if len(combo_list) == 0:
                self.show_property_metadata()
                return 0
        self.id_combo.clear()
        self.id_combo.addItems(combo_list)



    def id_meta_change(self):
        self.meta_id = self.id_combo.currentText()
        for i in reversed(range(self.result_meta_form_layout.count())):
            self.result_meta_form_layout.itemAt(i).widget().setParent(None)
        model = QtGui.QStandardItemModel()
        self.result_file_list_view.setModel(model)
        self.result_plot_view.setHtml("")
        self.current_plot = ""

        if self.meta_id == "" or self.meta_id == "None":
            return 0
        input_columns = self.current_mode_metadata_columns_info_df["display_text"].tolist()[2:]
        sql_columns = self.current_mode_metadata_columns_info_df["SQL_columns"].tolist()[2:]

        query = f"SELECT * FROM tfdb.{self.mode} WHERE id_{self.mode} = {self.meta_id};"
        with self.mysql_connection_open() as con_sql:
            df = pd.read_sql(query, con=con_sql)


        for i in range(len(input_columns)):
            input_column = input_columns[i]
            sql_column = sql_columns[i]
            element = str(df[sql_column][0])
            if sql_column == "Comment":
                widget = QTextEdit(element)
                widget.setReadOnly(True)
            elif sql_column == "Points":
                widget = QLabel(element)
            else:
                widget = QLineEdit(element)
                widget.setReadOnly(True)

            self.result_meta_form_layout.insertRow(i, input_column, widget)
        simple = self.current_mode_metadata_category_info_df["simple"][0]
        if simple:
            self.result_file_list_view.setVisible(False)
            with self.sftp_connection_open() as con_sftp:
                remote_path = f"/TFDB_drive/{self.mode}/{self.sample_id}-{self.meta_id}.csv"
                html = data_plot.visualize_remote_data_to_plot(con_sftp, remote_path, self.mode)
                self.result_plot_view.setHtml(html)
        else:
            with self.sftp_connection_open() as con_sftp:
                remote_directory = f"/TFDB_drive/{self.mode}/{self.sample_id}-{self.meta_id}/"

                try:
                    file_list = con_sftp.listdir(path = remote_directory)
                except IOError:
                    file_list = []
                    self.result_plot_view.setHtml('<html><head><body><p>data file does not exist</p></body></html>')
            self.result_file_list_view.setVisible(True)
            model = QtGui.QStandardItemModel()
            self.result_file_list_view.setModel(model)
            for i in file_list:
                item = QtGui.QStandardItem(i)
                model.appendRow(item)

    def result_file_list_view_double_clicked(self, index = None):
        if index == None:
            index = self.result_file_list_view.selectionModel().selectedIndexes()

        file_name = self.result_file_list_view.model().itemData(index)[0]
        full_remote_file_name = f"/TFDB_drive/{self.mode}/{self.sample_id}-{self.meta_id}/{file_name}"

        if self.current_plot == full_remote_file_name:
            pass
        else:
            with self.sftp_connection_open() as con_sftp:
                html = data_plot.visualize_remote_data_to_plot(con_sftp, full_remote_file_name, self.mode)
                self.result_plot_view.setHtml(html)
            self.current_plot = full_remote_file_name



    def result_meta_rep_show_points_button_clicked(self):
        possible_mode = ["XRD"]
        if self.mode in possible_mode:
            if self.rep_html != "":
                self.result_plot_view.setHtml(self.rep_html)

    def result_meta_data_show_button_clicked(self):
        possible_mode = ["XRD"]
        if self.mode in possible_mode:
            if self.rep_html != "":
                x = self.result_meta_rep_x_list_combo.currentText()
                y = self.result_meta_rep_y_list_combo.currentText()
                html = data_plot.show_XRD_plot_df(self.df, float(x), float(y))
                if html != 0:
                    self.result_plot_view.setHtml(html)

    def result_sample_edit_button_clicked(self):
        query = ""
        if self.result_sample_edit_button.text() == "edit":
            stylesheet = "background-color: rgb(255, 255, 127)"
            read_only = False
            self.result_sample_edit_button.setText("Cancel")
        else:
            stylesheet = ""
            read_only = True
            self.result_sample_edit_button.setText("edit")
            self.show_sample_metadata()

        total_columns = len(self.sample_columns_info_df) - 5


        for i in range(total_columns):
            index = i * 2 + 1
            widget = self.sample_meta_grid.itemAt(index).widget()
            widget.setStyleSheet(stylesheet)
            widget.setReadOnly(read_only)

        self.sample_meta_24.setStyleSheet(stylesheet)
        self.sample_meta_24.setReadOnly(read_only)

        self.result_sample_update_button.setDisabled(read_only)


    def result_sample_update_button_clicked(self):
        check = self.showDialog("Do you want to update the sample metadata?")
        if check:
            query = ""
            edited_meta_list = []

            total_columns = len(self.sample_columns_info_df) - 5

            df = self.sample_columns_info_df[4:].reset_index(drop=True)

            for i in range(total_columns):
                index = i * 2 + 1
                widget = self.sample_meta_grid.itemAt(index).widget()
                text = widget.text()
                edited_meta_list.append(text)
            edited_meta_list.append(self.sample_meta_24.toPlainText())

            sql_columns = [column for column in df["SQL_columns"]]
            print(sql_columns)

            with self.mysql_connection_open() as con_sql:
                success, message = connection.update_metadata(con_sql, "Sample", self.sample_id, edited_meta_list, sql_columns=sql_columns)
                self.sample_metadata = connection.get_sample_metadata(con_sql, self.sample_id, sql_columns)[1]
                print("Why is")
                print(self.sample_metadata)
            if success:
                self.showDialog("metadata successfully updated")
                self.result_sample_edit_button_clicked()
            else:
                self.error_dialog.showMessage(message)

    def result_sample_delete_button_clicked(self):

        check = self.showDialog("Do you really want to delete the whole sample data?")
        if check:
            with self.mysql_connection_open() as con_sql:
                query = f"SELECT `property`, `simple` FROM `tfdb_config`.`metadata_category`;"
                cursor = con_sql.cursor()
                cursor.execute(query)
                temp = cursor.fetchall()
                properties = [i[0] for i in temp]
                simple_list = [i[1] for i in temp]
            with self.mysql_connection_open() as con_sql, self.sftp_connection_open() as con_sftp:


                success = connection.delete_sample_by_id(con_sql, con_sftp, self.sample_id, properties, simple_list)
            if success == 1:
                self.showDialog("Sample successfully deleted")
                self.close()
            else:
                self.showDialog(success)

    def result_meta_edit_button_clicked(self):
        if self.meta_id == "" or self.meta_id == "None":
            pass
        else:
            if self.result_meta_edit_button.text() == "edit":
                stylesheet = "background-color: rgb(255, 255, 127)"
                read_only = False
                self.result_meta_edit_button.setText("Cancel")
            else:
                stylesheet = ""
                read_only = True
                self.result_meta_edit_button.setText("edit")

            sql_columns = self.current_mode_metadata_columns_info_df["SQL_columns"].tolist()[2:]
            for i in range(len(sql_columns)):
                widget = self.result_meta_form_layout.itemAt(i, 1).widget()
                widget_name = sql_columns[i]
                if widget_name == "Points":
                    pass
                else:
                    widget.setStyleSheet(stylesheet)
                    widget.setReadOnly(read_only)

            self.result_meta_update_button.setDisabled(read_only)

    def result_meta_update_button_clicked(self):
        check = self.showDialog("Do you want to update the metadata?")

        if check:

            sql_columns = self.current_mode_metadata_columns_info_df["SQL_columns"].tolist()[2:]
            sql_columns.remove("Points")
            edited_meta_list = []
            for i in range(len(sql_columns)):
                widget = self.result_meta_form_layout.itemAt(i, 1).widget()
                widget_name = sql_columns[i]
                if widget_name == "Comment":
                    text = widget.toPlainText()
                else:
                    text = widget.text()
                edited_meta_list.append(text)
            with self.mysql_connection_open() as con_sql:
                success, message = connection.update_metadata(con_sql, self.mode, self.meta_id, edited_meta_list, sql_columns)
                if success:
                    self.showDialog("metadata successfully updated")
                    self.id_meta_change()
                    self.result_meta_edit_button_clicked()
                else:
                    self.error_dialog.showMessage(message)

    def result_meta_delete_button_clicked(self):
        if self.delete_authority:
            remote_file_exists = True
            if self.mode != "None":
                check = self.showDialog("Do you want to delete this data?")
                if check:
                    simple = self.current_mode_metadata_category_info_df["simple"][0]
                    with self.mysql_connection_open() as con_sql, self.sftp_connection_open() as con_sftp:
                        try:
                            if simple:
                                remote_file_to_delete = f"/TFDB_drive/{self.mode}/{self.sample_id}-{self.meta_id}.csv"
                                try:
                                    con_sftp.remove(remote_file_to_delete)
                                except IOError:
                                    remote_file_exists = False
                            else:
                                remote_path = f"/TFDB_drive/{self.mode}/{self.sample_id}-{self.meta_id}/"
                                try:
                                    remote_file_list_to_delete = con_sftp.listdir(path=remote_path)
                                    for file in remote_file_list_to_delete:
                                        con_sftp.remove(remote_path + file)
                                    con_sftp.rmdir(remote_path)
                                except IOError:
                                    remote_file_exists = False


                            connection.delete_metadata_from_MySQL(con_sql, self.mode, self.meta_id,
                                                                  delete_authority=self.delete_authority)
                            self.mode_meta_change()
                            if not remote_file_exists:
                                self.showDialog("data file does not exist.")
                        except:
                            exception_message = str(sys.exc_info())
                            self.showDialog(exception_message)
            else:
                self.showDialog("There are no data to delete")

        else:
            self.showDialog("You do not have permission to delete")

    def get_and_show_sample_metadata(self):
        df = self.sample_columns_info_df[4:].reset_index(drop=True)
        sql_columns = [column for column in df["SQL_columns"]]
        with self.mysql_connection_open() as con_sql:
            success, self.sample_metadata = connection.get_sample_metadata(con_sql, self.sample_id, sql_columns)

        if success:
            self.show_sample_metadata()

    def show_sample_metadata(self):
        """`id_sample`, p.project_name, `Date`, e.name , `Target_Compo`, `P_B`, `P_LC`,
            `Deposition_temp`, `Anneal_Temp`, `Anneal_Time`, `Gas`, `P_Gas`, `Q_Gas`, `Gun_Angle`,
            `Substrate_Height`, `Rotation_Speed`, `Sputtering_Type`, `Power`,`Deposition_Time`, `Target_Thick`,
            `Sample_Shape`, `Silicon_Thick`, `Adh_Compo`, `Adh_Pow`, `Adh_time`, `Adh_Rotation`, `Substrate`,
            `Substrate_Thick`, `Comment`"""

        self.setWindowTitle("Sample-%s" % str(self.sample_metadata[0]))
        self.sample_meta_project.setText(str(self.sample_metadata[1]))
        self.sample_meta_date.setText(str(self.sample_metadata[2]))
        self.sample_meta_experimenter.setText(str(self.sample_metadata[3]))

        total_columns = len(self.sample_columns_info_df) - 5

        grid_list = self.sample_metadata[4:]

        for i in range(total_columns):
            index = i * 2 + 1
            widget = self.sample_meta_grid.itemAt(index).widget()
            widget.setText(str(grid_list[i]))

        self.sample_meta_24.setText(str(grid_list[total_columns]))



    def get_default_download_name(self):

        composition = self.sample_metadata[4].replace("/", "")


        default_download_name = f"{self.sample_id}-{self.meta_id}-{self.mode}-{composition}"
        return default_download_name


    @pyqtSlot()
    def data_download(self):
        if self.mode != "" and self.meta_id != "" and self.meta_id !="None":
            mode = self.mode
            with self.sftp_connection_open() as con_sftp:
                tag = f"{self.sample_id}-{self.meta_id}"
                simple = self.current_mode_metadata_category_info_df["simple"][0]
                default_download_name = self.get_default_download_name()
                if simple:
                    remote_path = f"/TFDB_drive/{mode}/{tag}.csv"
                    local_path = QFileDialog.getSaveFileName(self, "Data download", default_download_name, f"CSV file (*.csv)")[0]
                    if local_path:
                        try:
                            con_sftp.get(remote_path, local_path)
                            self.error_dialog.showMessage("File successfully downloaded")
                        except:
                            self.error_dialog.showMessage(str(sys.exc_info()))
                else:
                    remote_directory = f"/TFDB_drive/{mode}/{tag}/"
                    try:
                        remote_file_list = con_sftp.listdir(remote_directory)
                    except:
                        self.error_dialog.showMessage(str(sys.exc_info()))
                        return 0
                    local_directory = QFileDialog.getSaveFileName(self, "Data download", default_download_name, "")[0]
                    if local_directory:
                        os.mkdir(local_directory)
                        local_directory += "/"
                        try:
                            for remote_file in remote_file_list:
                                remote_path = remote_directory + remote_file
                                local_path = local_directory + remote_file
                                con_sftp.get(remote_path, local_path)
                            self.error_dialog.showMessage("File successfully downloaded")
                        except:
                            self.error_dialog.showMessage(str(sys.exc_info()))

        else:
            self.error_dialog.showMessage("meta_id not chosen")

    def showDialog(self, message):
        msgBox = QMessageBox()
        msgBox.setIcon(QMessageBox.Information)
        msgBox.setText(message)
        msgBox.setWindowTitle("Message Box")
        msgBox.setStandardButtons(QMessageBox.Ok | QMessageBox.Cancel)

        returnValue = msgBox.exec()
        if returnValue == QMessageBox.Ok:
            return True
        else:
            return False


if __name__ == '__main__':
    pass


