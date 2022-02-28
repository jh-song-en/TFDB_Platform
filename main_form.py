# -*- coding: utf-8 -*-
"""
Author: Jaeho Song
Author connection: jaeho.song.en@gmail.com
Description: Main form which shows the list of the sample and upload the data
             to the MySQL server and SFTP server.
"""
import sys
import os
import time
import re

# PyQt5 Libraries

from PyQt5 import QtWidgets, uic
from PyQt5 import QtGui
from PyQt5 import QtCore
from PyQt5 import QtWebEngineWidgets
from PyQt5.QtCore import pyqtSlot
from PyQt5.QtWidgets import *
from PyQt5.QtWidgets import QMessageBox

# Analytic libraries
import pandas as pd
import numpy as np
# Server connection
import mysql.connector as sql
from mysql.connector.locales.eng import client_error

# Local libraries
from DataFrameModel import DataFrameModel
from sub_form import sub_form
import data_plot
import connection


class main_form(QtWidgets.QMainWindow):
    def __init__(self, username, password, mysql_host, sftp_host, mysql_port, sftp_port, parent=None):
        QtWidgets.QMainWindow.__init__(self, parent)
        # Setting the position and size of the UI
        self.ui = uic.loadUi(os.path.abspath('source/main_form.ui'), self)
        self.setGeometry(300, 300, 1150, 574)

        # Error dialog
        self.error_dialog = QtWidgets.QErrorMessage()

        # Connection info

        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.sftp_host = sftp_host
        self.sftp_port = sftp_port

        self.username, self.password = username, password

        # initial_functions
        self.authority_setting()
        self.init_panel_tab(0)
        self.logbook_reset()

        self.logbook_table_view.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)

        # Search tab
        self.periodic_table_is_open = True
        self.search_result.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        self.periodic_table.setCurrentIndex(0)

        # Upload tab
        self.init_upload_tab()
        self.left_panel_summary_button_clicked()

        # Manage tab
        self.group = QButtonGroup()

        self.group.addButton(self.manage_radio_simple)
        self.group.addButton(self.manage_radio_locational)
        self.group.addButton(self.manage_radio_unristricted)

        self.last_clicked_panel_button = self.left_panel_tab_button_0
        self.prevButtonSearch.setEnabled(False)
        self.nextButtonSearch.setEnabled(False)
        self.ui.show()

        # Delete this function





    def authority_setting(self):
        with self.mysql_connection_open() as con_sql:
            self.delete_authority, self.insert_authority, self.update_authority = connection.authority_check(con_sql)
        stylesheet = "background-color: #262626;color:white;border:none;"
        if self.delete_authority:
            self.left_panel_tab_button_3.setText("Manager")
            self.left_panel_tab_button_3.setEnabled(True)
        elif self.insert_authority:
            self.left_panel_tab_button_3.setText("Researcher")
            self.left_panel_tab_button_3.setEnabled(False)

            self.left_panel_tab_button_3.setStyleSheet(stylesheet)
        else:
            self.left_panel_tab_button_3.setText("Visitor")
            self.left_panel_tab_button_3.setEnabled(False)
            self.left_panel_tab_button_3.setStyleSheet(stylesheet)

        self.logbook_save_button.setEnabled(self.insert_authority)
        self.upload_save_button.setEnabled(self.insert_authority)

    def mysql_connection_open(self):
        con_MySQL = connection.get_MySQL_Connection(self.mysql_host, self.mysql_port, 'tfdb', self.username,
                                                    self.password)
        return con_MySQL

    def sftp_connection_open(self):
        con_SFTP = connection.get_SFTP_Connection_paramiko(self.sftp_host, self.sftp_port, self.username, self.password)
        return con_SFTP

    """
    #######################################################################################
    Function below are for the left_panel
    
    The left panel manages the main_tab by clicking the lists of the tab on the panel
    #######################################################################################
    """

    def init_panel_tab(self, initial_panel_index):
        """
        This function initializes the attributes of the panel_tab
        """
        self.main_stacked_widget.setCurrentIndex(initial_panel_index)
        query = f"self.last_clicked_panel_button = self.left_panel_tab_button_{initial_panel_index}"
        exec(query)
        self.last_clicked_panel_button.setDisabled(True)

    @pyqtSlot()
    def panel_tab_buttons_clicked(self):
        """
        This function switches the current tab of main_stacked_widget

        self.left_panel_tab_button_0
            Text: Logbook
            Assigned tab index: 0

        self.left_panel_tab_button_1
            Text: Search
            Assigned tab index: 1

        self.left_panel_tab_button_2
            Text: Data
            Assigned tab index: 2
        """
        clicked_button = self.focusWidget()
        self.last_clicked_panel_button.setEnabled(True)
        clicked_button.setDisabled(True)
        if clicked_button == self.left_panel_tab_button_0:
            self.main_stacked_widget.setCurrentIndex(0)
        elif clicked_button == self.left_panel_tab_button_1:
            self.main_stacked_widget.setCurrentIndex(1)
        elif clicked_button == self.left_panel_tab_button_2:
            self.main_stacked_widget.setCurrentIndex(2)
        elif clicked_button == self.left_panel_tab_button_3:
            self.main_stacked_widget.setCurrentIndex(3)
            self.manage_init()

        self.last_clicked_panel_button = clicked_button

    @pyqtSlot()
    def left_panel_summary_button_clicked(self):

        text = ""

        with self.mysql_connection_open() as con_sql:
            data_summary_dict = connection.get_metadata_numbers(con_sql, self.mode_list)
        for key in data_summary_dict:
            text += f" {key}\t{data_summary_dict[key]}\n"
        self.left_panel_summary_edit.setText(text)

    """
    #######################################################################################
    Function below are for the logbook_tab
    
    The purpose of the logbook tab is to digitize the deposition record of the samples.
    The sample and its property data are bound by the id_sample in SQL database which is 
    given to the each samples when they are uploaded to the logbook. 
    
    Here, in the logbook_tab, experimenter can save the log of the sample deposition and
    browse conditions of the previously deposited samples. 
    #######################################################################################
    """

    def init_logbook_tab(self):
        """
        This function initializes the attributes of the logbook_tab
        """
        self.sample_meta_experimenter.clear()
        self.sample_meta_project.clear()
        with self.mysql_connection_open() as con_sql:
            self.project_df, self.experimenter_df = connection.get_project_and_experimenter_dataframe(con_sql)
        self.sample_meta_experimenter.addItems([""] + list(self.experimenter_df.index))
        self.sample_meta_project.addItems([""] + list(self.project_df.index))
        self.sample_meta_date.setText(time.strftime("%Y/%m/%d", time.localtime()))

    @pyqtSlot()
    def logbook_reset(self):
        """
        This function browse the first 20 recent samples on the logbook_table_view
        """

        self.set_logbook_sample_meta_grid()

        self.logbook_reset_button.setDisabled(True)
        with self.mysql_connection_open() as con_sql:
            sample_meta_count = connection.get_sample_total_number(con_sql)
        self.set_logbook_maximum_page(sample_meta_count)
        self.logbook_current_page_edit.setValidator(QtGui.QIntValidator(1, self.logbook_total_pages))
        self.logbook_current_page = 1
        self.logbook_current_page_edit.setText('1')
        self.show_logbook(1)
        self.logbook_reset_button.setEnabled(True)
        self.init_logbook_tab()
        self.sample_meta_comment.setText("")

    def set_logbook_sample_meta_grid(self):
        self.sample_columns_info_df = self.get_current_info_table("Sample", only_df=True)
        total_columns = len(self.sample_columns_info_df) - 5
        div = 4
        grid_rows = int(total_columns / div) + (total_columns % div > 0)

        df = self.sample_columns_info_df[4:].reset_index(drop=True)

        for i in reversed(range(self.sample_meta_grid.count())):
            self.sample_meta_grid.itemAt(i).widget().setParent(None)


        for i in range(total_columns):

            display_text = df["display_text"][i]
            input_class = df["input_class"][i]
            row = i % grid_rows
            column = (i // grid_rows) * 2
            self.sample_meta_grid.addWidget(QLabel(display_text), row, column)
            self.sample_meta_grid.addWidget(QLabel(input_class), row, column + 1)
            if input_class == "QLineEdit":
                widget = QLineEdit()
            elif input_class == "QComboBox":
                widget = QComboBox()
                widget.setEditable(True)
                combo_list = df["combo_list"][i].split("/")
                widget.addItems(combo_list)
            self.sample_meta_grid.addWidget(widget, row, column + 1)


    def show_logbook(self, page):
        """
        This function browse the 20 samples on the logbook_table_view
        """
        # Enable every buttons first
        self.logbook_prev_button.setEnabled(True)
        self.logbook_next_button.setEnabled(True)

        select_query = "SELECT `id_sample`, p.project_name, `Date`, e.name AS Person, "
        from_query = """\nFROM `tfdb`.`Sample` AS s
                        JOIN `tfdb`.`Project` AS p ON s.id_project = p.id_project
                        JOIN `tfdb`.`Experimenter` AS e ON s.id_experimenter = e.id_experimenter 
                        ORDER BY id_sample DESC LIMIT %d,20;""" % (20 * (page - 1))

        middle_query = ", ".join(["`" + column + "`" for column in self.sample_columns_info_df["SQL_columns"]][4:])

        select_query + from_query + middle_query
        with self.mysql_connection_open() as con_sql:
            sample_df = pd.read_sql(select_query + middle_query + from_query , con=con_sql)
        model = DataFrameModel(sample_df)
        self.logbook_table_view.setModel(model)
        self.logbook_current_page_edit.setText(str(self.logbook_current_page))
        # button disable
        if self.logbook_current_page >= self.logbook_total_pages:
            self.logbook_next_button.setEnabled(False)
        if self.logbook_current_page <= 1:
            self.logbook_prev_button.setEnabled(False)

    def logbook_table_view_double_clicked(self):
        index = self.logbook_table_view.selectionModel().selectedIndexes()
        model = self.logbook_table_view.model()
        metadata_list = [model.itemData(ele)[0] for ele in index]
        self.set_logbook_input(metadata_list[4:])

    def set_logbook_input(self, metadata_list):
        total_columns = len(self.sample_columns_info_df) - 5
        for i in range(total_columns):
            index = i * 3 + 2
            widget = self.sample_meta_grid.itemAt(index).widget()

            input_class = self.sample_meta_grid.itemAt(index - 1).widget().text()
            if input_class == "QLineEdit":
                widget.setText(metadata_list[i])
            elif input_class == "QComboBox":
                widget.setCurrentText(metadata_list[i])
        self.sample_meta_comment.setText(metadata_list[total_columns])


    def set_logbook_maximum_page(self, count, page_limit=20):
        """
        This function sets maximum page of the logbook by its number and page limit.
        """
        self.logbook_prev_button.setEnabled(False)
        self.logbook_next_button.setEnabled(True)
        self.logbook_total_pages = count // page_limit
        if count % page_limit != 0:
            self.logbook_total_pages += 1
        if self.logbook_total_pages <= 1:
            self.logbook_next_button.setEnabled(False)
        self.logbook_total_page_label.setText("/ " + str(self.logbook_total_pages))

    @pyqtSlot()
    def logbook_next_button_click(self):
        self.logbook_current_page += 1
        self.show_logbook(self.logbook_current_page)

    @pyqtSlot()
    def logbook_prev_button_click(self):
        self.logbook_current_page -= 1
        self.show_logbook(self.logbook_current_page)

    @pyqtSlot()
    def logbook_save_button_click(self):
        try:
            project = self.sample_meta_project.currentText()
            experimenter = self.sample_meta_experimenter.currentText()
            if project == "" or experimenter == "":
                self.error_dialog.showMessage("Project name or person information required")
                return 0

            check = self.showDialog("Do you want to save the sample metadata?")
            if check:
                # todo: sample_metadata format check
                sample_metadata = self.get_sample_metadata_list()
                if not self.metadata_validity_check("Sample", sample_metadata):
                    self.error_dialog.showMessage("Wrong Meta data format. Please check the file")
                    return 0
                # connection.upload_metadata returns last input id, if error, returns error message
                sql_columns = self.sample_columns_info_df["SQL_columns"].tolist()[1:]
                with self.mysql_connection_open() as con_sql:
                    last_input_id = connection.upload_metadata_to_MySQL(con_sql, "Sample", sample_metadata, sql_columns=sql_columns)
                if isinstance(last_input_id, int):
                    self.showDialog("Sample metadata saved as id_sample: %d" % last_input_id)
                else:
                    self.error_dialog.showMessage(last_input_id)

        except:
            self.error_dialog.showMessage(str(sys.exc_info()))

        self.show_logbook(1)

    def get_sample_metadata_list(self):
        total_columns = len(self.sample_columns_info_df) - 5
        metadata_list = [str(self.project_df.loc[self.sample_meta_project.currentText(), 'id_project']),
                        self.sample_meta_date.text(),
                        str(self.experimenter_df.loc[self.sample_meta_experimenter.currentText(), 'id_experimenter'])]

        for i in range(total_columns):
            index = i * 3 + 2
            widget = self.sample_meta_grid.itemAt(index).widget()

            input_class = self.sample_meta_grid.itemAt(index - 1).widget().text()
            if input_class == "QLineEdit":
                text = widget.text()
            elif input_class == "QComboBox":
                text = widget.currentText()
            metadata_list.append(text)
        comment = self.sample_meta_comment.text()
        metadata_list.append(comment)
        metadata_list

        return metadata_list



    """
    #######################################################################################
    Function below are the functions for the search_tab
    
    search_tab offers the list of the samples which fits the search condition. The user can
    search 
    #######################################################################################
    """

    def search_search_type_compo_changed(self):
        search_type = self.search_search_type_combo.currentText()
        if search_type == "Composition" or search_type == "ID":
            if search_type == "Composition":
                self.periodic_table.setCurrentIndex(0)
            else:
                self.periodic_table.setCurrentIndex(1)

            self.search_option_btn.setEnabled(True)
            if self.periodic_table_is_open:
                self.periodic_table.setVisible(True)
            else:
                self.periodic_table.setVisible(False)
        else:
            if self.periodic_table_is_open:
                self.periodic_table.setVisible(False)
            self.search_option_btn.setEnabled(False)

    def periodic_table_click(self):
        if self.periodic_table_is_open:
            self.periodic_table.setVisible(False)
            self.periodic_table_is_open = False
        else:
            self.periodic_table.setVisible(True)
            self.periodic_table_is_open = True

    def search_search_button_clicked(self):
        """
        This function opens result form, class sub_Form()
        """

        self.search_keyword = self.search_search_bar_edit.text()
        self.search_type = self.search_search_type_combo.currentText()
        with self.mysql_connection_open() as con_sql:
            count = connection.advanced_search(con_sql, self.search_keyword, self.search_type, self.mode_list,
                                               get_total_number=True)

        self.set_search_maximum_page(count)
        self.search_current_page = 1
        self.SearchCurrentPage.setText('1')
        self.show_search_page(1)

    @pyqtSlot()
    def search_next_page(self):
        self.search_current_page += 1
        self.show_search_page(self.search_current_page)

    @pyqtSlot()
    def search_prev_page(self):
        self.search_current_page -= 1
        self.show_search_page(self.search_current_page)

    def show_search_page(self, page):
        composition = 0

        try:
            # enable every buttons first
            self.prevButtonSearch.setEnabled(True)
            self.nextButtonSearch.setEnabled(True)

            df = self.sample_columns_info_df[["SQL_columns","search"]]

            with self.mysql_connection_open() as con_sql:
                sample_df = connection.advanced_search(con_sql, self.search_keyword, self.search_type, self.mode_list,
                                                       page=page, row_per_page=20, search_column_setting_df=df)
            model = DataFrameModel(sample_df)
            self.search_result.setModel(model)
            self.SearchCurrentPage.setText(str(self.search_current_page))
            # button disable
            if self.search_current_page >= self.search_total_pages:
                self.nextButtonSearch.setEnabled(False)
            if self.search_current_page <= 1:
                self.prevButtonSearch.setEnabled(False)
        except:
            self.error_dialog.showMessage(str(sys.exc_info()))

    def set_search_maximum_page(self, count, page_limit=20):
        self.prevButtonSearch.setEnabled(False)
        self.nextButtonSearch.setEnabled(True)
        self.search_total_pages = count // page_limit
        if count % page_limit != 0:
            self.search_total_pages += 1
        if self.search_total_pages <= 1:
            self.nextButtonSearch.setEnabled(False)
        self.SearchTotalPage.setText("/ " + str(self.search_total_pages))

    def open_sample_window(self, item):
        row = item.row()
        model = self.search_result.model()
        index = model.index(row, 0)
        sample_id = int(model.data(index))
        self.sample_window = sub_form(self.username, self.password, self.mysql_host, self.sftp_host, self.mysql_port,
                                      self.sftp_port, sample_id, parent=None)

    def sample_composition_button_clicked(self):
        button = self.sender()
        button_text = button.text()
        if button_text == "Clear":
            self.search_search_bar_edit.setText("")
        else:
            search_type = self.search_search_type_combo.currentText()
            search_keyword = self.search_search_bar_edit.text()
            if search_type == "Composition":
                if search_keyword == "":
                    self.search_search_bar_edit.setText(search_keyword + button_text)
                else:

                    self.search_search_bar_edit.setText(search_keyword + " " + button_text)
            elif search_type == "ID":
                self.search_search_bar_edit.setText(search_keyword + button_text)

    """
    #######################################################################################
    Function below are the functions for upload tab

    Upload tab uploads both metadata and data on the mysql server and file server.
    With the given sample id, user can upload measured property data in this section.
    #######################################################################################
    """

    def metadata_category_info_to_df(self, mode):
        query = f"SELECT * FROM tfdb_config.metadata_category WHERE `property` = '{mode}';"
        with self.mysql_connection_open() as con_sql:
            try:
                metadata_category_info_df = pd.read_sql(query, con=con_sql)
                return metadata_category_info_df
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)

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

    def init_upload_tab(self):
        query = f"SELECT `property` FROM tfdb_config.metadata_category;"
        with self.mysql_connection_open() as con_sql:
            try:
                cursor = con_sql.cursor()
                cursor.execute(query)
                temp = cursor.fetchall()
                self.mode_list = [i[0] for i in temp]
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
        self.upload_mode_combobox.clear()
        self.upload_mode_combobox.addItems(self.mode_list)

    def upload_mode_combobox_changed(self):

        self.mode = self.upload_mode_combobox.currentText()
        if self.mode == "":
            return 0
        self.upload_reset()
        # Initialize self.upload_meta_form_layout
        for i in reversed(range(self.upload_meta_form_layout.count())):
            self.upload_meta_form_layout.itemAt(i).widget().setParent(None)
        # Get settings from dictionary
        self.current_mode_metadata_category_info_df = self.metadata_category_info_to_df(self.mode)
        self.current_mode_metadata_columns_info_df = self.metadata_columns_info_to_df(self.mode)
        "id_columns display_text input_class SQL_columns  SQL_datatype   combo_list"
        input_columns = self.current_mode_metadata_columns_info_df["display_text"].tolist()[1:]
        input_class = self.current_mode_metadata_columns_info_df["input_class"].tolist()[1:]
        temp = self.current_mode_metadata_columns_info_df["combo_list"].tolist()[1:]
        combo_list = [temp_str.split("/") for temp_str in temp]

        # add items in self.upload_meta_form_layout
        for i in range(len(input_columns)):
            widget_name = input_class[i]
            if widget_name == "QLineEdit":
                widget = QLineEdit()
            elif widget_name == "QTextEdit":
                widget = QTextEdit()
            elif widget_name == "QLabel":
                widget = QLabel()
            elif widget_name == "QComboBox":
                widget = QComboBox()
                widget.addItems(combo_list[i])
            self.upload_meta_form_layout.insertRow(i, input_columns[i], widget)

    def upload_property_input_metadata(self):
        input_class = self.current_mode_metadata_columns_info_df["input_class"].tolist()
        input_class.pop(0)

        metadata_input = []
        for i in range(len(input_class)):
            widget = self.upload_meta_form_layout.itemAt(i, 1).widget()
            widget_name = input_class[i]
            if widget_name == "QLineEdit":
                metadata_input.append(widget.text())
            elif widget_name == "QTextEdit":
                metadata_input.append(widget.toPlainText())
            elif widget_name == "QLabel":
                metadata_input.append(widget.text())
            elif widget_name == "QComboBox":
                metadata_input.append(widget.currentText())
        return metadata_input

    def manage_simple_radio_button_toggled(self):
        toggled = self.manage_radio_simple.isChecked()
        if toggled:
            self.manage_data_type_edit.setText("CSV Files")
            self.manage_extension_edit.setText("csv")

        self.manage_data_type_edit.setDisabled(toggled)
        self.manage_extension_edit.setDisabled(toggled)

    def upload_load_button_clicked(self):

        simple = self.current_mode_metadata_category_info_df["simple"][0]
        data_type = self.current_mode_metadata_category_info_df["data_type"][0]
        allowed_extensions = self.current_mode_metadata_category_info_df["allowed_extensions"][0].strip()
        extension_list = ["*." + ext for ext in allowed_extensions.split("/")]
        data_type = data_type + "(" + " ".join(extension_list) + ")"

        file_selected = False
        if simple:
            file_path = [QFileDialog.getOpenFileName(self, self.tr("Load File"), "", self.tr(data_type))[0]]
            if file_path[0]: file_selected = True
        else:
            file_path = QFileDialog.getOpenFileNames(self, self.tr("Load Files"), "", self.tr(data_type))[0]
            if file_path: file_selected = True

        if file_selected:
            self.directory = os.path.dirname(file_path[0])
            self.file_list = [os.path.basename(file) for file in file_path]
            self.upload_file_directory.setText(self.directory)
            model = QtGui.QStandardItemModel()
            self.upload_file_list_view.setModel(model)
            for i in self.file_list:
                item = QtGui.QStandardItem(i)
                model.appendRow(item)

            self.upload_plot_view_show_plot()

    def upload_plot_view_show_plot(self):
        simple = self.current_mode_metadata_category_info_df["simple"][0]
        xy_coordinate = self.current_mode_metadata_category_info_df["xy_coordinate"][0]
        html, points = data_plot.visualize_data(self.mode, self.directory, self.file_list, simple, xy_coordinate)
        input_columns = self.current_mode_metadata_columns_info_df["display_text"].tolist()
        input_columns.pop(0)
        index = -1
        for i in range(len(input_columns)):
            if input_columns[i] == "Points":
                index = i
        if index >= 0:
            widget = self.upload_meta_form_layout.itemAt(index, 1).widget()
            widget.setText(str(points))
        self.upload_plot_view.setHtml(html)

    def upload_file_list_view_double_clicked(self):
        index = self.upload_file_list_view.selectionModel().selectedIndexes()

        try:
            directory = self.upload_file_directory.text()
            file_name = self.upload_file_list_view.model().itemData(index[0])[0]
            full_path = directory + "/" + file_name

            os.popen(full_path)
        except:
            self.error_dialog.showMessage(str(sys.exc_info()))

    @pyqtSlot()
    def upload_save_button_clicked(self):  # todo save section
        """
        Data upload protocol checklist
        Does the sample exist?
        Bring sample data and get permission
        Is the file in right format?
        Is the meta-data in right format?
        Save meta-data into Database
            ->get meta-data id
        Save file into sftp(sample_id-md_id.format)
        Check sftp (if not, delete metadata in Database)
        Print(“Data saved”)
        De activate save button
        """
        self.upload_save_button.setEnabled(False)
        self.upload_load_button.setEnabled(False)

        metadata_list = self.upload_property_input_metadata()
        id_sample = metadata_list[0]
        # todo: metadata format check
        if not self.metadata_validity_check(self.mode, metadata_list):
            self.error_dialog.showMessage("Wrong Meta data format. Please check the file")
            self.upload_save_button.setEnabled(True)
            self.upload_load_button.setEnabled(True)
            return 0
        # File format check

        simple = self.current_mode_metadata_category_info_df["simple"][0]
        xy_coordinate = self.current_mode_metadata_category_info_df["xy_coordinate"][0]
        success, error_message = data_plot.file_format_check(self.mode, self.directory, self.file_list, simple,
                                                             xy_coordinate)

        if not success:
            self.error_dialog.showMessage(error_message)
            self.upload_reset()
            return 0
        # Sample metadata validity check
        df = self.sample_columns_info_df[4:].reset_index(drop=True)
        sql_columns = [column for column in df["SQL_columns"]]

        with self.mysql_connection_open() as con_sql, self.sftp_connection_open() as con_sftp:
            exists, existence_message = connection.Sample_existance_check(con_sql, id_sample, sql_columns)

            if exists:
                sample_check_answer = self.showDialog(existence_message)
                if not sample_check_answer:
                    self.upload_save_button.setEnabled(True)
                    self.upload_load_button.setEnabled(True)
                    return 0
            else:
                self.error_dialog.showMessage(existence_message)
                self.upload_save_button.setEnabled(True)
                self.upload_load_button.setEnabled(True)
                return 0

            sql_columns = self.current_mode_metadata_columns_info_df["SQL_columns"].tolist()[1:]
            last_input_id = connection.upload_metadata_to_MySQL(con_sql, self.mode, metadata_list,
                                                                sql_columns=sql_columns)

            simple = self.current_mode_metadata_category_info_df["simple"][0]
            if not isinstance(last_input_id, int):
                self.error_dialog.showMessage(last_input_id)
                self.upload_reset()
                return 0
            success, error_message = connection.upload_data_to_sftp(con_sql, con_sftp, self.mode,
                                                                    self.directory, id_sample, last_input_id,
                                                                    self.delete_authority, self.file_list, simple, self)
        if success:
            self.showDialog("Data successfully uploaded")

        else:
            self.error_dialog.showMessage(error_message)
        self.upload_reset()

    def upload_reset(self):
        self.directory = ""
        self.file_list = []
        self.upload_file_directory.setText(self.directory)
        self.upload_plot_view.setHtml("")
        model = QtGui.QStandardItemModel()
        self.upload_file_list_view.setModel(model)
        self.upload_progress_bar.setValue(0)
        self.upload_save_button.setEnabled(True)
        self.upload_load_button.setEnabled(True)

    def upload_progress_bar_control(self, progress):
        self.upload_progress_bar.setValue(progress)

    """
    Sample_info_manage
    """

    def manage_init(self):
        # self.manage_initial_function()
        self.get_current_info_table("Project")
        self.get_current_info_table("Person")
        self.get_current_info_table("Sample")

    def get_current_info_table(self, mode, only_df=False):
        if mode == "Project":
            query = "SELECT * FROM `tfdb`.`Project`;"
        elif mode == "Person":
            query = "SELECT * FROM `tfdb`.`Experimenter`;"
        elif mode == "Sample":
            query = "SELECT * FROM `tfdb_config`.`sample_columns_info`;"
        with self.mysql_connection_open() as con_sql:
            try:
                df = pd.read_sql(query, con=con_sql)
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
                return 0
        if only_df:
            return df

        self.set_current_info_table(df, mode)

    def set_current_info_table(self, df, mode):
        if mode == "Project":
            self.manage_project_table_widget.clear()
            self.manage_project_table_widget.setRowCount(0)
            self.manage_project_table_widget.setColumnCount(3)
            self.manage_project_table_widget.verticalHeader().setVisible(False)

            columns = ["id_project", "Project name", "Description"]
            self.manage_project_table_widget.setHorizontalHeaderLabels(columns)
            for index, row in df.iterrows():
                self.manage_project_table_widget.insertRow(index)
                id_project = str(row["id_project"])
                project_name = row["project_name"]
                project_description = row["project_description"]
                item = QTableWidgetItem(id_project)
                item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
                self.manage_project_table_widget.setItem(index, 0, item)
                item = QTableWidgetItem(project_name)
                self.manage_project_table_widget.setItem(index, 1, item)
                item = QTableWidgetItem(project_description)
                self.manage_project_table_widget.setItem(index, 2, item)

        elif mode == "Person":
            self.manage_person_table_widget.clear()
            self.manage_person_table_widget.setRowCount(0)
            self.manage_person_table_widget.setColumnCount(3)
            self.manage_person_table_widget.verticalHeader().setVisible(False)

            columns = ["id_person", "Name", "Description"]
            self.manage_person_table_widget.setHorizontalHeaderLabels(columns)
            for index, row in df.iterrows():
                self.manage_person_table_widget.insertRow(index)
                id_experimenter = str(row["id_experimenter"])
                name = row["name"]
                description = row["description"]
                item = QTableWidgetItem(id_experimenter)
                item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
                self.manage_person_table_widget.setItem(index, 0, item)
                item = QTableWidgetItem(name)
                self.manage_person_table_widget.setItem(index, 1, item)
                item = QTableWidgetItem(description)
                self.manage_person_table_widget.setItem(index, 2, item)

        elif mode == "Sample":
            self.manage_sample_table_widget.clear()
            row_len = len(df)
            self.manage_sample_table_widget.setRowCount(row_len)
            self.manage_sample_table_widget.setColumnCount(7)
            self.manage_sample_table_widget.verticalHeader().setVisible(False)

            columns = ["id_columns", "Display text", "Input class", "SQL columns",
                                    "SQL datatype", "Combo list", "Search"]
            self.manage_sample_table_widget.setHorizontalHeaderLabels(columns)
            for index, row in df.iterrows():

                previous_order = str(row["id_columns"])
                display_text = row["display_text"]
                input_class = row["input_class"]
                sql_columns = row["SQL_columns"]
                sql_datatype = row["SQL_datatype"]
                combo_list = row["combo_list"]
                search = row["search"]
                editable = True

                if sql_columns in ["id_sample", "Date", "id_project", "id_experimenter", "Comment"]:
                    editable = False

                self.set_sample_manage_table_widget_row(index, editable, previous_order, display_text, input_class,
                                                 sql_columns,
                                                 sql_datatype, combo_list, search)
            pass


    def set_sample_manage_table_widget_row(self, index, editable, id_columns, display_text, input_class, sql_columns,
                                    sql_datatype, combo_list, search):
        item = QTableWidgetItem(id_columns)
        item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
        self.manage_sample_table_widget.setItem(index, 0, item)

        item = QTableWidgetItem(display_text)
        if not editable: item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
        self.manage_sample_table_widget.setItem(index, 1, item)

        if editable:
            combo = QComboBox()
            combo.setEditable(False)
            combo.addItems(["QLineEdit", "QComboBox"])
            combo.setCurrentText(input_class)
            self.manage_sample_table_widget.setCellWidget(index, 2, combo)
        else:
            item = QTableWidgetItem(input_class)
            item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
            self.manage_sample_table_widget.setItem(index, 2, item)

        item = QTableWidgetItem(sql_columns)
        if not editable: item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
        self.manage_sample_table_widget.setItem(index, 3, item)

        if editable:
            combo = QComboBox()
            combo.setEditable(True)
            combo.addItems(["INT()", "VARCHAR()", "DECIMAL(,)"])
            combo.setCurrentText(sql_datatype)
            self.manage_sample_table_widget.setCellWidget(index, 4, combo)
        else:
            item = QTableWidgetItem(sql_datatype)
            item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
            self.manage_sample_table_widget.setItem(index, 4, item)

        item = QTableWidgetItem(combo_list)
        if not editable: item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
        self.manage_sample_table_widget.setItem(index, 5, item)

        item = QTableWidgetItem(search)
        self.manage_sample_table_widget.setItem(index, 6, item)

    def manage_general_add_row_button_clicked(self):

        clicked_button = self.focusWidget()
        if clicked_button == self.manage_project_add_row_button:
            adding_row = self.manage_project_table_widget.rowCount()
            self.manage_project_table_widget.insertRow(adding_row)
            item = QTableWidgetItem("")
            item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
            self.manage_project_table_widget.setItem(adding_row, 0, item)
            item = QTableWidgetItem("")
            self.manage_project_table_widget.setItem(adding_row, 1, item)
            item = QTableWidgetItem("")
            self.manage_project_table_widget.setItem(adding_row, 2, item)
            self.manage_project_table_widget.scrollToBottom()
        elif clicked_button == self.manage_person_add_row_button:
            adding_row = self.manage_person_table_widget.rowCount()
            self.manage_person_table_widget.insertRow(adding_row)
            item = QTableWidgetItem("")
            item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
            self.manage_person_table_widget.setItem(adding_row, 0, item)
            item = QTableWidgetItem("")
            self.manage_person_table_widget.setItem(adding_row, 1, item)
            item = QTableWidgetItem("")
            self.manage_person_table_widget.setItem(adding_row, 2, item)
            self.manage_person_table_widget.scrollToBottom()
        elif clicked_button == self.manage_sample_add_row_button:
            selected_row = self.manage_sample_table_widget.currentRow()
            a_item = self.manage_sample_table_widget.item(selected_row, 3)
            content = str(a_item.text())
            if content in ["id_sample", "Date", "id_project", "Comment"]:
                return 0
            self.manage_sample_table_widget.insertRow(selected_row + 1)
            self.set_sample_manage_table_widget_row(selected_row + 1, True, "", "New Column", "QLineEdit", "new_column",
                                             "INT()","","")



    def manage_general_delete_row_button_clicked(self):
        clicked_button = self.focusWidget()
        if clicked_button == self.manage_project_delete_row_button:
            selected_row = self.manage_project_table_widget.currentRow()
            if selected_row < 0:
                return 0
            id_project = self.manage_project_table_widget.item(selected_row, 0).text()
            if id_project != "":
                query = f"SELECT * FROM `tfdb`.`Sample` WHERE id_project = {id_project};"
                with self.mysql_connection_open() as con_sql:
                    try:
                        df = pd.read_sql(query, con=con_sql)
                        if len(df) > 0:
                            self.error_dialog.showMessage(
                                "The data related to this project exists. The project can't be deleted.")
                            return 0
                    except:
                        exception_message = str(sys.exc_info())
                        self.error_dialog.showMessage(exception_message)
                        return 0

            self.manage_project_table_widget.removeRow(selected_row)
        elif clicked_button == self.manage_person_delete_row_button:
            selected_row = self.manage_person_table_widget.currentRow()
            if selected_row < 0:
                return 0
            id_person = self.manage_person_table_widget.item(selected_row, 0).text()
            if id_person != "":
                query = f"SELECT * FROM `tfdb`.`Sample` WHERE id_experimenter = {id_person};"
                with self.mysql_connection_open() as con_sql:
                    try:
                        df = pd.read_sql(query, con=con_sql)
                        if len(df) > 0:
                            self.error_dialog.showMessage(
                                "The data related to this project exists. The project can't be deleted.")
                            return 0
                    except:
                        exception_message = str(sys.exc_info())
                        self.error_dialog.showMessage(exception_message)
                        return 0

            self.manage_person_table_widget.removeRow(selected_row)
        elif clicked_button == self.manage_sample_delete_row_button:

            selected_row = self.manage_sample_table_widget.currentRow()
            a_item = self.manage_sample_table_widget.item(selected_row, 3)
            content = str(a_item.text())
            if content in ["id_sample", "Date", "id_project", "id_experimenter", "Comment"]:
                return 0

            self.manage_sample_table_widget.removeRow(selected_row)

    def manage_general_apply_button_clicked(self):
        clicked_button = self.focusWidget()
        if clicked_button == self.manage_project_apply_button:
            current_row_count = self.manage_project_table_widget.rowCount()
            last_id = 0
            df = self.get_current_info_table("Project", only_df=True)
            deleted_index_list = [df["id_project"][i] for i in range(len(df))]
            update_list = []
            input_list = []
            for i in range(current_row_count):
                id_string = self.manage_project_table_widget.item(i, 0).text()
                if id_string != "":
                    id = int(id_string)
                    deleted_index_list.remove(id)
                    if id > last_id:
                        last_id = id
                    row_data = [self.manage_project_table_widget.item(i, 1).text(),
                                self.manage_project_table_widget.item(i, 2).text(),
                                self.manage_project_table_widget.item(i, 0).text()]
                    update_list.append(row_data)
                else:
                    last_id += 1
                    name = self.manage_project_table_widget.item(i, 1).text()
                    if len(name) < 2:
                        self.error_dialog.showMessage(
                            f"The length of the project name is too short. it must be longer than 2 characters ({name})")
                        return 0
                    row_data = [str(last_id), name,
                                self.manage_project_table_widget.item(i, 2).text()]
                    input_list.append(row_data)

            with self.mysql_connection_open() as con_sql:
                try:
                    cursor = con_sql.cursor()
                    for deleted_index in deleted_index_list:
                        delete_query = "DELETE FROM `tfdb`.`Project` WHERE `id_project` = %s"
                        cursor.execute(delete_query, [str(deleted_index)])
                    for update_row_list in update_list:
                        update_query = "UPDATE `tfdb`.`Project` SET `project_name` = %s, `project_description` = %s WHERE (`id_project` = %s);"
                        cursor.execute(update_query, update_row_list)
                    for input_row_list in input_list:
                        edited_query = "INSERT INTO `tfdb`.`Project` (`id_project`, `project_name`, `project_description`) VALUES (%s, %s, %s);"
                        cursor.execute(edited_query, input_row_list)
                    con_sql.commit()
                    cursor.close()
                except:
                    exception_message = str(sys.exc_info())
                    self.error_dialog.showMessage(exception_message)

            self.get_current_info_table("Project")

        elif clicked_button == self.manage_person_apply_button:
            current_row_count = self.manage_person_table_widget.rowCount()
            last_id = 0
            df = self.get_current_info_table("Person", only_df=True)
            deleted_index_list = [df["id_experimenter"][i] for i in range(len(df))]
            update_list = []
            input_list = []
            for i in range(current_row_count):
                id_string = self.manage_person_table_widget.item(i, 0).text()

                if id_string != "":
                    id = int(id_string)
                    deleted_index_list.remove(id)
                    if id > last_id:
                        last_id = id
                    row_data = [self.manage_person_table_widget.item(i, 1).text(),
                                self.manage_person_table_widget.item(i, 2).text(),
                                self.manage_person_table_widget.item(i, 0).text()]
                    update_list.append(row_data)
                else:
                    last_id += 1
                    name = self.manage_person_table_widget.item(i, 1).text()
                    if len(name) < 2:
                        self.error_dialog.showMessage(
                            f"The length of the person name is too short. it must be longer than 2 characters ({name})")
                        return 0
                    row_data = [str(last_id), name,
                                self.manage_person_table_widget.item(i, 2).text()]
                    input_list.append(row_data)

            with self.mysql_connection_open() as con_sql:
                try:
                    cursor = con_sql.cursor()
                    for deleted_index in deleted_index_list:
                        delete_query = "DELETE FROM `tfdb`.`Experimenter` WHERE `id_experimenter` = %s"
                        cursor.execute(delete_query, [str(deleted_index)])
                    for update_row_list in update_list:
                        update_query = "UPDATE `tfdb`.`Experimenter` SET `name` = %s, `description` = %s WHERE (`id_experimenter` = %s);"
                        cursor.execute(update_query, update_row_list)
                    for input_row_list in input_list:
                        edited_query = "INSERT INTO `tfdb`.`Experimenter` (`id_experimenter`, `name`, `description`) VALUES (%s, %s, %s);"
                        cursor.execute(edited_query, input_row_list)
                    con_sql.commit()
                    cursor.close()
                except:
                    exception_message = str(sys.exc_info())
                    self.error_dialog.showMessage(exception_message)

            self.get_current_info_table("Person")
            pass

        elif clicked_button == self.manage_sample_apply_button:
            check = self.showDialog( "Data can be lost during the modification. "
                                     "Do you want to change the database setting?")
            if check:
                df = self.get_current_info_table("Sample", only_df=True)
                edited_df = self.get_edited_sample_manage_table_df()
                verified = self.verify_edited_sample_manage_table_df(edited_df)
                if verified:
                    self.modify_sample_table_columns_info(df, edited_df)
                    self.get_current_info_table("Sample")
                self.logbook_reset()
                return 0

    def get_edited_sample_manage_table_df(self):
        column_list = ["id_columns", "display_text", "input_class", "SQL_columns", "SQL_datatype", "combo_list", "sample"]
        data = []
        current_row_count = self.manage_sample_table_widget.rowCount()
        for i in range(current_row_count):
            row_data = []
            for j in range(len(column_list)):
                a_item = self.manage_sample_table_widget.item(i, j)
                if a_item:
                    content = str(a_item.text())
                else:
                    content = str(self.manage_sample_table_widget.cellWidget(i, j).currentText())
                row_data.append(content)
            data.append(row_data)
        edited_df = pd.DataFrame(data, columns=column_list)
        return edited_df

    def verify_edited_sample_manage_table_df(self, edited_df):
        if len(edited_df[edited_df['display_text'] == ''].index) > 5:
            self.error_dialog.showMessage("Display text column have missing values")
            return False
        if len(edited_df[edited_df['SQL_columns'] == ''].index) > 0:
            self.error_dialog.showMessage("SQL columns column have missing values")
            return False
        if not edited_df["display_text"][4:].str.lower().is_unique:
            self.error_dialog.showMessage("Display text column must have unique values")
            return False
        if not edited_df["SQL_columns"].str.lower().is_unique:
            self.error_dialog.showMessage("SQL columns column must have unique values")
            return False

        for index, row in edited_df.iterrows():
            data_type_string = row["SQL_datatype"]
            if not self.metadata_columns_sql_datatype_format_check(index, data_type_string, date=True):
                self.error_dialog.showMessage(
                    f"Row {index} is in wrong format ({data_type_string}) on column \"SQL_columns\"")
                return False
        return True

    def modify_sample_table_columns_info(self, df, edited_df):
        alter_query = f"ALTER TABLE `tfdb`.`Sample` \n"
        drop_query_list = []
        add_query_list = []
        change_query_list = []
        deleted_index_list = [df["id_columns"][i] for i in range(len(df))]
        for index, row in edited_df.iterrows():

            edited_sql_column_name = row["SQL_columns"]
            sql_datatype = row["SQL_datatype"]
            id_columns = row["id_columns"]
            if id_columns != "":
                deleted_index_list.remove(int(id_columns))
            if index == 0:
                query = f"CHANGE COLUMN `{edited_sql_column_name}` `{edited_sql_column_name}` {sql_datatype} NOT NULL " \
                        f"AUTO_INCREMENT FIRST"
                change_query_list.append(query)
            elif edited_sql_column_name in ["id_project", "id_experimenter"]:
                previous_sql_column_name = edited_df["SQL_columns"][index - 1]
                query = f"CHANGE COLUMN `{edited_sql_column_name}` `{edited_sql_column_name}` {sql_datatype} NOT NULL " \
                        f"AFTER `{previous_sql_column_name}`"
                change_query_list.append(query)
            else:
                previous_sql_column_name = edited_df["SQL_columns"][index - 1]
                if id_columns == "":
                    query = f"ADD `{edited_sql_column_name}` {sql_datatype} NULL AFTER `{previous_sql_column_name}`"
                    add_query_list.append(query)
                else:
                    current_sql_column_name = df["SQL_columns"][int(id_columns)]
                    query = f"CHANGE COLUMN `{current_sql_column_name}` `{edited_sql_column_name}` {sql_datatype} NULL AFTER `{previous_sql_column_name}`"
                    change_query_list.append(query)

        for index in deleted_index_list:
            current_sql_column_name = df["SQL_columns"][index]
            query = f"DROP COLUMN `{current_sql_column_name}`"
            drop_query_list.append(query)

        total_query_list = drop_query_list + add_query_list + change_query_list
        total_query = alter_query + ",\n".join(total_query_list) + ";"
        delete_query = "Truncate table `tfdb_config`.`sample_columns_info`;"

        with self.mysql_connection_open() as con_sql:
            try:
                cursor = con_sql.cursor()
                cursor.execute(total_query)
                cursor.execute(delete_query)
                for index, row in edited_df.iterrows():
                    input_list = list(row)
                    input_list[0] = str(index)
                    edited_query = "INSERT INTO `tfdb_config`.`sample_columns_info` " \
                                   "(`id_columns`, `display_text`, `input_class`, `SQL_columns`, `SQL_datatype`, `combo_list`, `search`) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s);"


                    cursor.execute(edited_query, input_list)
                con_sql.commit()
                cursor.close()
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
                return False

    def manage_property_manage_button_clicked(self):
        self.main_stacked_widget.setCurrentIndex(4)
        self.manage_initial_function()
    """
    Manage tab
    """

    def manage_initial_function(self, new_property=""):
        query = "SELECT property FROM tfdb_config.metadata_category;"
        with self.mysql_connection_open() as con_sql:
            try:
                cursor = con_sql.cursor()
                cursor.execute(query)
                metadata_list = [metadata_tube[0] for metadata_tube in cursor.fetchall()]
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
        if new_property:
            metadata_list.insert(0, new_property)
        self.manage_metadata_combo.clear()
        self.manage_metadata_combo.addItems(metadata_list)

    def manage_create_property(self, new_property):

        """
        id_Property must be set
        """
        self.manage_table_widget.clear()
        rows = 4
        columns = 6
        self.manage_table_widget.setColumnCount(columns)
        self.manage_table_widget.setRowCount(rows)
        self.manage_table_widget.verticalHeader().setVisible(False)
        columns = ["previous order", "Display text", "Input class", "SQL columns", "SQL datatype", "combo list"]
        self.manage_table_widget.setHorizontalHeaderLabels(columns)
        self.manage_table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        id_property = "id_" + new_property
        self.set_manage_table_widget_row(0, False, "", "", "", id_property, "INT(11)", "")
        self.set_manage_table_widget_row(1, False, "", "Sample ID", "QLineEdit", "id_sample", "INT(11)", "")
        self.set_manage_table_widget_row(2, False, "", "Comment", "QTextEdit", "Comment", "VARCHAR(400)", "")
        self.set_manage_table_widget_row(3, False, "", "Points", "QLabel", "Points", "INT(11)", "")

        self.manage_data_type_edit.setText("")
        self.manage_extension_edit.setText("")
        self.group.setExclusive(False)
        self.manage_radio_simple.setChecked(False)
        self.manage_radio_locational.setChecked(False)
        self.manage_radio_unristricted.setChecked(False)
        self.group.setExclusive(True)

    def manage_initial_function(self, new_property=""):
        query = "SELECT property FROM tfdb_config.metadata_category;"
        with self.mysql_connection_open() as con_sql:
            try:
                cursor = con_sql.cursor()
                cursor.execute(query)
                metadata_list = [metadata_tube[0] for metadata_tube in cursor.fetchall()]
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
        if new_property:
            metadata_list.insert(0, new_property)
        self.manage_metadata_combo.clear()
        self.manage_metadata_combo.addItems(metadata_list)

    def manage_add_new_database_button_clicked(self):

        new_property, ok = QInputDialog.getText(self, 'input dialog', 'New database name:')
        if ok:
            if not (len(new_property) > 3 and new_property.isalpha()):
                self.error_dialog.showMessage(
                    "The property name should be only written in alphabet letters and longer than 3 letters. (No space)")
                return 0
            query = "SHOW TABLES FROM tfdb;"
            with self.mysql_connection_open() as con_sql:
                try:
                    cursor = con_sql.cursor()
                    cursor.execute(query)
                    table_list = [metadata_tube[0].lower() for metadata_tube in cursor.fetchall()]

                except:
                    exception_message = str(sys.exc_info())
                    self.error_dialog.showMessage(exception_message)
                    return 0

            if new_property.lower() not in table_list:
                self.manage_initial_function(new_property=new_property)
            else:
                self.error_dialog.showMessage("The table already exists.")

    def manage_metadata_combo_changed(self):
        selected_property = self.manage_metadata_combo.currentText()
        if selected_property == "":
            return 0
        query = "SELECT property FROM tfdb_config.metadata_category;"
        with self.mysql_connection_open() as con_sql:
            try:
                cursor = con_sql.cursor()
                cursor.execute(query)
                metadata_list = [metadata_tube[0] for metadata_tube in cursor.fetchall()]
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
        if selected_property not in metadata_list:
            self.manage_create_property(selected_property)
            return 0

        query = f"SELECT * FROM tfdb_config.metadata_category WHERE `property`=\'{selected_property}\';"

        with self.mysql_connection_open() as con_sql:
            try:
                df = pd.read_sql(query, con=con_sql)
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)

        self.manage_data_type_edit.setText(df["data_type"][0])
        self.manage_extension_edit.setText(df["allowed_extensions"][0])

        simple = df["simple"][0]
        xy_coordinate = df["xy_coordinate"][0]
        metadata_id = df["id_metadata"][0]

        if simple == 1:
            self.manage_radio_simple.setChecked(True)
        else:
            if xy_coordinate == 1:
                self.manage_radio_locational.setChecked(True)
            else:
                self.manage_radio_unristricted.setChecked(True)

        df_columns_info = self.get_metadata_columns_info(metadata_id)
        self.manage_table_widget.clear()
        rows = df_columns_info.shape[0]
        columns = 6
        self.manage_table_widget.setColumnCount(columns)
        self.manage_table_widget.setRowCount(rows)
        self.manage_table_widget.verticalHeader().setVisible(False)
        self.manage_table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        columns = ["previous order", "Display text", "Input class", "SQL columns", "SQL datatype", "combo list"]
        self.manage_table_widget.setHorizontalHeaderLabels(columns)

        for index, row in df_columns_info.iterrows():
            editable = True
            if index < 2 or rows - index < 3:
                editable = False
            previous_order = row["order_integer"]
            display_text = row["display_text"]
            input_class = row["input_class"]
            sql_columns = row["SQL_columns"]
            sql_datatype = row["SQL_datatype"]
            combo_list = row["combo_list"]
            self.set_manage_table_widget_row(index, editable, previous_order, display_text, input_class, sql_columns,
                                             sql_datatype, combo_list)

    def get_metadata_columns_info(self, metadata_id):
        query = f"SELECT * FROM tfdb_config.metadata_columns_info WHERE id_columns LIKE \'%-{metadata_id}\' ORDER BY cast(id_columns as unsigned) ASC;"
        with self.mysql_connection_open() as con_sql:
            try:
                df_columns_info = pd.read_sql(query, con=con_sql)
                df_columns_info[["order_integer", "id_property"]] = df_columns_info["id_columns"].str.split("-",
                                                                                                            expand=True)
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
        return df_columns_info

    def manage_add_row_button_clicked(self):
        current_row_count = self.manage_table_widget.rowCount()
        selected_row = self.manage_table_widget.currentRow()
        if selected_row < 1 or current_row_count - selected_row < 3:
            return 0
        self.manage_table_widget.insertRow(selected_row + 1)
        self.set_manage_table_widget_row(selected_row + 1, True, "", "New Column", "QLineEdit", "new_column", "INT()",
                                         "")

    def manage_delete_row_button_clicked(self):
        current_row_count = self.manage_table_widget.rowCount()
        selected_row = self.manage_table_widget.currentRow()
        if selected_row < 1 or current_row_count - selected_row < 3:
            return 0
        self.manage_table_widget.removeRow(selected_row)

    def manage_apply_button_clicked(self):
        edited_metadata_category_df, edited_metadata_columns_info_df = self.get_metadata_setting_values()
        metadata_category_verified = self.verify_edited_metadata_category_df(edited_metadata_category_df)
        if not metadata_category_verified:
            return 0
        metadata_columns_info_verified = self.verify_edited_metadata_columns_info_df(edited_metadata_columns_info_df)
        if not metadata_columns_info_verified:
            return 0
        query = "SHOW TABLES FROM tfdb;"
        with self.mysql_connection_open() as con_sql:
            try:
                cursor = con_sql.cursor()
                cursor.execute(query)
                table_list = [metadata_tube[0].lower() for metadata_tube in cursor.fetchall()]
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
                return 0
        property_name = edited_metadata_category_df["property"][0]
        if property_name.lower() not in table_list:
            # success = self.create_metadata_table_and_sftp_folder(property_name, edited_metadata_columns_info_df)
            success = self.create_metadata_info(edited_metadata_category_df, edited_metadata_columns_info_df)

            if not success:
                self.error_dialog.showMessage("Failed to create new property database.")
                return 0

        else:
            check = self.showDialog(
                "Data can be lost during the modification. Do you want to change the database setting?")
            if not check:
                return 0
            query = f"SELECT * FROM tfdb_config.metadata_category WHERE property = \'{property_name}\';"
            with self.mysql_connection_open() as con_sql:
                try:
                    metadata_category_df = pd.read_sql(query, con=con_sql,
                                                       columns=["id_metadata", "property", "simple", "xy_coordinate",
                                                                "data_type", "allowed_extensions"])
                except:
                    exception_message = str(sys.exc_info())
                    self.error_dialog.showMessage(exception_message)
                    return 0
            id_metadata = metadata_category_df["id_metadata"][0]
            edited_metadata_category_df["id_metadata"][0] = id_metadata

            same = True
            for col in ["id_metadata", "property", "simple", "xy_coordinate", "data_type", "allowed_extensions"]:
                a = metadata_category_df[col][0]
                b = edited_metadata_category_df[col][0]
                if a != b:
                    same = False
            if not same:
                success = self.modify_metadata_category(id_metadata, edited_metadata_category_df)
                if not success:
                    return 0
            self.modify_metadata_columns_info(id_metadata, property_name, edited_metadata_columns_info_df)
        self.showDialog("Database successfully modified.")
        self.manage_initial_function()
        self.init_upload_tab()

    def create_metadata_info(self, edited_metadata_category_df, edited_metadata_columns_info_df):
        query = "INSERT INTO `tfdb_config`.`metadata_category` (`property`, `simple`, `xy_coordinate`, `data_type`, `allowed_extensions`) " \
                "VALUES (%s, %s, %s, %s, %s);"
        a = [str(edited_metadata_category_df["property"][0]),
             str(edited_metadata_category_df["simple"][0]),
             str(edited_metadata_category_df["xy_coordinate"][0]),
             str(edited_metadata_category_df["data_type"][0]),
             str(edited_metadata_category_df["allowed_extensions"][0])]

        property_name = a[0]

        create_query = f"CREATE TABLE `tfdb`.`{property_name}` (\n"
        middle_query = ""
        for index, row in edited_metadata_columns_info_df.iterrows():
            edited_sql_column_name = row["SQL_columns"]
            sql_datatype = row["SQL_datatype"]
            if index == 0:
                middle_query += f"`{edited_sql_column_name}` {sql_datatype} NOT NULL AUTO_INCREMENT,\n"
                primary_key = edited_sql_column_name
            elif index == 1:
                middle_query += f"`{edited_sql_column_name}` {sql_datatype} NOT NULL,\n"
            else:
                middle_query += f"`{edited_sql_column_name}` {sql_datatype} NULL,\n"

        end_query = f"PRIMARY KEY (`{primary_key}`),\n" \
                    "INDEX `id_sample_idx` (`id_sample` ASC),\n" \
                    "CONSTRAINT `id_sample`\n" \
                    "FOREIGN KEY (`id_sample`)\n" \
                    "REFERENCES `tfdb`.`Sample` (`id_sample`)\n" \
                    "ON DELETE NO ACTION\n" \
                    "ON UPDATE NO ACTION);"
        total_query = create_query + middle_query + end_query

        with self.mysql_connection_open() as con_sql, self.sftp_connection_open() as con_sftp:
            try:
                cursor = con_sql.cursor()
                cursor.execute(total_query)
                cursor.execute(query, a)
                cursor.execute("SELECT LAST_INSERT_ID();")
                last_inserted_id = cursor.fetchall()
                id_metadata = last_inserted_id[0][0]
                for index, row in edited_metadata_columns_info_df.iterrows():
                    input_list = list(row)
                    input_list[0] = f"{index}-{id_metadata}"
                    edited_query = "INSERT INTO `tfdb_config`.`metadata_columns_info` (`id_columns`, `display_text`, `input_class`, `SQL_columns`, `SQL_datatype`, `combo_list`) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s);"

                    cursor.execute(edited_query, input_list)

                remote_directory = f"/TFDB_drive/{property_name}"
                con_sftp.mkdir(remote_directory)

                con_sql.commit()
                cursor.close()
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
                return False
        return True

    def modify_metadata_category(self, id_metadata, edited_metadata_category_df):
        input_list = [str(edited_metadata_category_df["property"][0]),
                      str(edited_metadata_category_df["simple"][0]),
                      str(edited_metadata_category_df["xy_coordinate"][0]),
                      str(edited_metadata_category_df["data_type"][0]),
                      str(edited_metadata_category_df["allowed_extensions"][0])]

        if id_metadata:
            input_list.append(str(id_metadata))
            query = "UPDATE `tfdb_config`.`metadata_category` SET `property` = %s, `simple` = %s," \
                    " `xy_coordinate` = %s, `data_type` = %s, `allowed_extensions` = %s " \
                    "WHERE (`id_metadata` = %s);"
        else:
            pass
        with self.mysql_connection_open() as con_sql:
            try:
                cursor = con_sql.cursor()
                cursor.execute(query, input_list)
                con_sql.commit()

            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
                return False
        return True

    def modify_metadata_columns_info(self, id_metadata, property_name, edited_metadata_columns_info_df):
        # Generate query with edited_metadata_columns_info_df
        alter_query = f"ALTER TABLE `tfdb`.`{property_name}` \n"
        drop_query_list = []
        add_query_list = []
        change_query_list = []
        current_metadata_columns_info_df = self.get_metadata_columns_info(id_metadata)
        deleted_index_list = [i for i in range(len(current_metadata_columns_info_df))]

        for index, row in edited_metadata_columns_info_df.iterrows():
            edited_sql_column_name = row["SQL_columns"]
            sql_datatype = row["SQL_datatype"]
            id_columns = row["id_columns"]
            if id_columns != "":
                deleted_index_list.remove(int(id_columns))
            if index == 0:
                query = f"CHANGE COLUMN `{edited_sql_column_name}` `{edited_sql_column_name}` {sql_datatype} NOT NULL AUTO_INCREMENT FIRST"
                change_query_list.append(query)
            elif index == 1:
                previous_sql_column_name = edited_metadata_columns_info_df["SQL_columns"][index - 1]
                query = f"CHANGE COLUMN `{edited_sql_column_name}` `{edited_sql_column_name}` {sql_datatype} NOT NULL AFTER `{previous_sql_column_name}`"
                change_query_list.append(query)
            else:
                previous_sql_column_name = edited_metadata_columns_info_df["SQL_columns"][index - 1]
                if id_columns == "":
                    query = f"ADD `{edited_sql_column_name}` {sql_datatype} NULL AFTER `{previous_sql_column_name}`"
                    add_query_list.append(query)
                else:
                    current_sql_column_name = current_metadata_columns_info_df["SQL_columns"][int(id_columns)]
                    query = f"CHANGE COLUMN `{current_sql_column_name}` `{edited_sql_column_name}` {sql_datatype} NULL AFTER `{previous_sql_column_name}`"
                    change_query_list.append(query)
        for index in deleted_index_list:
            current_sql_column_name = current_metadata_columns_info_df["SQL_columns"][index]
            query = f"DROP COLUMN `{current_sql_column_name}`"
            drop_query_list.append(query)

        total_query_list = drop_query_list + add_query_list + change_query_list
        total_query = alter_query + ",\n".join(total_query_list) + ";"
        delete_query = f"DELETE FROM tfdb_config.metadata_columns_info WHERE id_columns LIKE \'%-{id_metadata}\';"

        with self.mysql_connection_open() as con_sql:
            try:
                cursor = con_sql.cursor()
                cursor.execute(total_query)
                cursor.execute(delete_query)
                for index, row in edited_metadata_columns_info_df.iterrows():
                    input_list = list(row)
                    input_list[0] = f"{index}-{id_metadata}"
                    edited_query = "INSERT INTO `tfdb_config`.`metadata_columns_info` (`id_columns`, `display_text`, `input_class`, `SQL_columns`, `SQL_datatype`, `combo_list`) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s);"

                    cursor.execute(edited_query, input_list)
                con_sql.commit()
                cursor.close()
            except:
                exception_message = str(sys.exc_info())
                self.error_dialog.showMessage(exception_message)
                return False

    def verify_edited_metadata_category_df(self, edited_metadata_category_df):
        if edited_metadata_category_df["data_type"][0] == "":
            self.error_dialog.showMessage("Please input the data type.")
            return False
        if edited_metadata_category_df["allowed_extensions"][0] == "":
            self.error_dialog.showMessage("Please input the extension type")
            return False
        if edited_metadata_category_df["simple"][0] == None:
            self.error_dialog.showMessage("Please choose the category.")
            return False

        if not re.match("^[a-zA-Z0-9/]*$", edited_metadata_category_df["allowed_extensions"][0]):
            self.error_dialog.showMessage("You can oly type alphabet, numbers, and / on the extension edit. (No space)")
            return False

        return True

    def verify_edited_metadata_columns_info_df(self, edited_metadata_columns_info_df):
        if len(edited_metadata_columns_info_df[edited_metadata_columns_info_df['display_text'] == ''].index) > 1:
            self.error_dialog.showMessage("Display text column have missing values")
            return False
        if len(edited_metadata_columns_info_df[edited_metadata_columns_info_df['SQL_columns'] == ''].index) > 0:
            self.error_dialog.showMessage("SQL columns column have missing values")
            return False
        if not edited_metadata_columns_info_df["display_text"].str.lower().is_unique:
            self.error_dialog.showMessage("Display text column must have unique values")
            return False
        if not edited_metadata_columns_info_df["SQL_columns"].str.lower().is_unique:
            self.error_dialog.showMessage("SQL columns column must have unique values")
            return False

        for index, row in edited_metadata_columns_info_df.iterrows():
            data_type_string = row["SQL_datatype"]
            if not self.metadata_columns_sql_datatype_format_check(index, data_type_string):
                self.error_dialog.showMessage(
                    f"Row {index} is in wrong format ({data_type_string}) on column \"SQL_columns\"")
                return False
        return True

    def metadata_columns_sql_datatype_format_check(self, i, data_type_string, date=False):
        if date == True:
            if data_type_string.lower() == "date":
                return True
        if re.fullmatch(".*\(.*\)", data_type_string) != None and data_type_string.count(
                "(") == 1 and data_type_string.count(")") == 1:
            res = re.split('\(|\)', data_type_string)
            if res[0].lower() == "int" or res[0].lower() == "varchar":
                if not res[1].isdigit():
                    return False
            elif res[0].lower() == "decimal":
                a = res[1].split(",")
                if not (int(a[0]) >= int(a[1])):
                    return False
                if re.fullmatch("\d+,\d+", res[1]) == None:
                    return False
            else:
                return False
        else:
            return False
        return True

    def get_metadata_setting_values(self):
        data_type = self.manage_data_type_edit.text()
        extension = self.manage_extension_edit.text()

        if self.manage_radio_simple.isChecked():
            simple = 1
            xy_coordinate = 1
        elif self.manage_radio_locational.isChecked():
            simple = 0
            xy_coordinate = 1
        elif self.manage_radio_unristricted.isChecked():
            simple = 0
            xy_coordinate = 0
        else:
            simple = None
            xy_coordinate = None
        edited_metadata_category = [
            [None, self.manage_metadata_combo.currentText(), simple, xy_coordinate, data_type, extension]]

        edited_metadata_columns = ["id_metadata", "property", "simple", "xy_coordinate", "data_type",
                                   "allowed_extensions"]
        edited_metadata_category_df = pd.DataFrame(edited_metadata_category, columns=edited_metadata_columns)

        current_row_count = self.manage_table_widget.rowCount()
        column_list = ["id_columns", "display_text", "input_class", "SQL_columns", "SQL_datatype", "combo_list"]
        data = []
        for i in range(current_row_count):
            row_data = []
            for j in range(len(column_list)):
                a_item = self.manage_table_widget.item(i, j)
                if a_item:
                    content = str(a_item.text())
                else:
                    content = str(self.manage_table_widget.cellWidget(i, j).currentText())
                row_data.append(content)
            data.append(row_data)

        edited_metadata_columns_info_df = pd.DataFrame(data, columns=column_list)
        return edited_metadata_category_df, edited_metadata_columns_info_df

    def set_manage_table_widget_row(self, index, editable, previous_order, display_text, input_class, sql_columns,
                                    sql_datatype, combo_list):
        item = QTableWidgetItem(previous_order)
        item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
        self.manage_table_widget.setItem(index, 0, item)

        item = QTableWidgetItem(display_text)
        if not editable: item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
        self.manage_table_widget.setItem(index, 1, item)

        if editable:
            combo = QComboBox()
            combo.setEditable(False)
            combo.addItems(["QLineEdit", "QComboBox"])
            combo.setCurrentText(input_class)
            self.manage_table_widget.setCellWidget(index, 2, combo)
        else:
            item = QTableWidgetItem(input_class)
            item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
            self.manage_table_widget.setItem(index, 2, item)

        item = QTableWidgetItem(sql_columns)
        if not editable: item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
        self.manage_table_widget.setItem(index, 3, item)

        if editable:
            combo = QComboBox()
            combo.setEditable(True)
            combo.addItems(["INT()", "VARCHAR()", "DECIMAL(,)"])
            combo.setCurrentText(sql_datatype)
            self.manage_table_widget.setCellWidget(index, 4, combo)
        else:
            item = QTableWidgetItem(sql_datatype)
            item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
            self.manage_table_widget.setItem(index, 4, item)

        item = QTableWidgetItem(combo_list)
        if not editable: item.setFlags(QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled)
        self.manage_table_widget.setItem(index, 5, item)

    def manage_cancel_button_clicked(self):
        self.main_stacked_widget.setCurrentIndex(3)
        #todo

    """
    Message Box
    """

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

    """
    Close event
    """

    def closeEvent(self, event):
        pass



    def get_metadata_list(self, mode):
        """
        metadata_list rule:
        except for Sample,
        """

        if mode == "Sample":
            metadata_list = [
                str(self.project_df.loc[self.sample_meta_project.currentText(), 'id_project']),  # 0   : Project
                self.sample_meta_date.text(),  # 1   : Date
                str(self.experimenter_df.loc[self.sample_meta_experimenter.currentText(), 'id_experimenter']),
                # 2   : Experimenter

                self.sample_meta_0.text(),  # 2  : Target Composition
                self.sample_meta_1.text(),  # 3   : P_B
                self.sample_meta_2.text(),  # 4   : P_LC
                self.sample_meta_3.text(),  # 5   : Deposition Temperature
                self.sample_meta_4.text(),  # 6   : Annealing Temperature
                self.sample_meta_5.text(),  # 7   : Annealing_Time
                self.sample_meta_6.currentText(),  # 8   : Gas
                self.sample_meta_7.text(),  # 9   : P_gas
                self.sample_meta_8.text(),  # 10  : Q_gas
                self.sample_meta_9.text(),  # 11  : Gun angle
                self.sample_meta_10.text(),  # 12  : Substrate height
                self.sample_meta_11.text(),  # 13  : Rotation Speed
                self.sample_meta_12.currentText(),  # 14  : Sputtering Type
                self.sample_meta_13.text(),  # 15  : Power
                self.sample_meta_14.text(),
                self.sample_meta_15.text(),  # 17  : Target Thickness
                self.sample_meta_16.text(),  # 18  : Sample shape
                self.sample_meta_17.text(),  # 19  : Silicon Thickness
                self.sample_meta_18.text(),  # 20  : Adh.layer Composition
                self.sample_meta_19.text(),  # 21  : Adh.layer Power
                self.sample_meta_20.text(),  # 22  : Adh.layer time
                self.sample_meta_21.text(),  # 23  : Adh.Rotation
                self.sample_meta_22.currentText(),  # 24  : Substrate
                self.sample_meta_23.text(),  # 25  : Substrate Thickness
                self.sample_meta_24.text()  # 26  : Comment
            ]
        else:
            metadata_list = []
        return metadata_list

    def metadata_validity_check(self, mode, metadata_list):
        return True
