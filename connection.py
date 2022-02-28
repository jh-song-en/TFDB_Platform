import mysql.connector as sql
from mysql.connector.locales.eng import client_error
import sys
import os
import paramiko
import re
from pandas import read_sql
import pandas as pd
import base64


def get_MySQL_Connection(host, port, database, username, password):
    connection_MySQL = sql.connect(host=host, port=port, database=database,
                                   user=username, password=password)
    return connection_MySQL


def get_SFTP_Connection_paramiko(host, port, username, password):
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy)
    cli.connect(host, port=port, username=username, password=password)
    connection_sftp = cli.open_sftp()
    return connection_sftp


def get_sample_total_number(connector_SQL):
    cursor = connector_SQL.cursor()
    # This Query returns tuple list [(1,)] if exist [(0,)]
    cursor.execute(
        "SELECT COUNT(*) FROM tfdb.Sample;")
    count = cursor.fetchall()
    return count[0][0]


def get_project_and_experimenter_dataframe(connector_SQL):
    project_df = read_sql('SELECT id_project, project_name FROM Project;',
                          con=connector_SQL).set_index('project_name')
    experimenter_df = read_sql('SELECT * FROM Experimenter;', con=connector_SQL).set_index('name')

    return project_df, experimenter_df


def Sample_existance_check(connector_SQL, id_sample, sql_columns):
    """This funcnction checks if the sample exists.
    return 1 if exists, 0 if not
    """
    try:
        int_id_sample = int(id_sample)
    except ValueError:
        return 0, "Sample id not valid"
    cursor = connector_SQL.cursor()
    # This Query returns tuple list [(1,)] if exist [(0,)]
    cursor.execute(
        "SELECT EXISTS (SELECT `id_sample` FROM tfdb.Sample WHERE `id_sample` LIKE %s) AS success;" % int_id_sample)
    exists = cursor.fetchall()
    if exists[0][0]:
        query = "SELECT `id_sample`, p.project_name, `Date`, e.name , "
        query += ", ".join(["`" + column + "`" for column in sql_columns])
        query += """
                FROM `tfdb`.`Sample` AS s
                JOIN `tfdb`.`Project` AS p
                ON s.id_project = p.id_project
                JOIN `tfdb`.`Experimenter` AS e
                ON s.id_experimenter = e.id_experimenter
                WHERE id_sample LIKE %s;
                """ % int_id_sample
        try:
            cursor.execute(query)
            sample_data = cursor.fetchall()
        except:
            return 1, str(sys.exc_info())

        columns = ['id_sample', 'project_name', 'Date', 'Person'] + sql_columns
        string = ""
        for i in range(len(columns)):
            if i % 3 == 0:
                string += "\n"
            string += columns[i] + ": " + str(sample_data[0][i]) + "\t"

        message = "Do you want to save the data for the sample?\n Sample data:"

        return 1, message + string
    else:
        return 0, "Sample Doesn't exist"


def last_input_id(connector_SQL):
    """This function returns last inserted id in SQL    
    """
    try:
        cursor = connector_SQL.cursor()
        # This Query returns tuple list [(int,)]
        cursor.execute("SELECT LAST_INSERT_ID();")
        last_inserted_id = cursor.fetchall()
    except:
        exception_message = str(sys.exc_info())
        return exception_message
    return last_inserted_id[0][0]


def upload_metadata_to_MySQL(connector_SQL, mode, metadata_list, sql_columns=None):
    cursor = connector_SQL.cursor()

    query = f"INSERT INTO `tfdb`.`{mode}`"
    column_query = " ("
    values_query = " VALUES ("
    for i in range(len(sql_columns)):
        column_query += "`" + sql_columns[i] + "`"
        values_query += "%s"
        if i != len(sql_columns) - 1:
            column_query += ", "
            values_query += ","
        else:
            column_query += ")"
            values_query += ");"
    query = query + column_query + values_query

    try:
        cursor.execute(query, metadata_list)
        connector_SQL.commit()
    except:
        exception_message = str(sys.exc_info())
        return exception_message
    return last_input_id(connector_SQL)


def upload_data_to_sftp(connector_SQL, connector_sftp, mode, directory, id_sample, id_metadata, delete_authority,
                        file_list, simple, main_form):
    full_path_list = [directory + "/" + file for file in file_list]

    if simple:
        full_path = full_path_list[0]
        extension = os.path.splitext(full_path)[1]
        remote_file_name = f"{id_sample}-{id_metadata}{extension}"
        remote_path = f"/TFDB_drive/{mode}/{remote_file_name}"
        try:
            connector_sftp.put(full_path, remote_path)
            connector_sftp.stat(remote_path)
            main_form.upload_progress_bar_control(100)
        except:
            exception_message = str(sys.exc_info())
            delete_metadata_from_MySQL(connector_SQL, mode, id_metadata, delete_authority)
            return False, exception_message
    else:
        try:
            remote_directory = f"/TFDB_drive/{mode}/{id_sample}-{id_metadata}"
            connector_sftp.mkdir(remote_directory)
            total_num = len(full_path_list)
            for i in range(total_num):
                remote_path = remote_directory + "/" + f"{file_list[i]}"
                connector_sftp.put(full_path_list[i], remote_path)
                connector_sftp.stat(remote_path)
                progress = int(((i + 1) / total_num) * 100)
                main_form.upload_progress_bar_control(progress)
        except:
            exception_message = str(sys.exc_info())
            delete_metadata_from_MySQL(connector_SQL, mode, id_metadata, delete_authority)
            return False, exception_message
    return True, ""


def xy_to_csv_format(file_path, x, y):
    with open(file_path, mode='r') as f:
        while True:
            alpha_in = False
            line = f.readline()
            for letter in line:
                if letter.isalpha():
                    alpha_in = True
            if not alpha_in:
                file_string = line + f.read()
                break

        if file_string.endswith('\n'):
            file_string = file_string[:-2]
        return f'{x},{y},' + file_string.replace(' ', ',').replace('\n', f'\n{x},{y},')


def merge_xy_files(directory, file_list):
    merged_data = "X,Y,2Theta,peak\n"
    for file_name in file_list:
        lst = re.split("\(|,|\)", file_name.replace(" ", ""))
        x = lst[1]
        y = lst[2]
        full_path = directory + '/' + file_name

        data = xy_to_csv_format(full_path, x, y)

        merged_data = merged_data + data + '\n'
    return merged_data


def download_data_from_sftp(connector_sftp, mode, full_path, remote_path):
    try:
        connector_sftp.get(remote_path, full_path)
    except:
        exception_message = str(sys.exc_info())


def delete_metadata_from_MySQL(connector, mode, id_metadata, delete_authority):
    if delete_authority:
        query = f"DELETE FROM `tfdb`.`{mode}` WHERE id_{mode} = {id_metadata}"
    else:
        query = f"UPDATE `tfdb`.`{mode}` SET `id_sample` = 0 WHERE id_{mode} = {id_metadata}"
    cursor = connector.cursor()
    try:
        cursor.execute(query)
        connector.commit()
    except:
        exception_message = str(sys.exc_info())
        return exception_message
    return 1


def delete_sample_by_id(con_SQL, con_sftp, id_sample, properties, single_list):
    for i in range(len(properties)):
        mode = properties[i]
        single = single_list[i]
        success, file_list = get_file_list(con_sftp, mode)
        for file_name in file_list:
            if file_name.startswith(f"{id_sample}-"):
                remote_path = f"/TFDB_drive/{mode}/{file_name}"
                try:
                    if single:
                        con_sftp.remove(remote_path)
                    else:
                        remote_file_list_to_delete = con_sftp.listdir(path=remote_path)
                        for file in remote_file_list_to_delete:
                            con_sftp.remove(remote_path + '/' + file)
                        con_sftp.rmdir(remote_path)
                except:
                    exception_message = str(sys.exc_info())
                    return exception_message
        success, file_list = get_file_list(con_sftp, mode)
    properties = ["Sample"] + properties
    for mode in properties:
        query = f"DELETE FROM `tfdb`.`{mode}` WHERE id_sample = {id_sample}"
        cursor = con_SQL.cursor()
        try:
            cursor.execute(query)
            con_SQL.commit()
        except:
            exception_message = str(sys.exc_info())
            return exception_message
    return 1


def get_sample_metadata(connector_SQL, id_sample, sql_columns):
    """This funcnction checks if the sample exists.
    return 1 if exists, 0 if not
    """
    try:
        int_id_sample = int(id_sample)
    except ValueError:
        return 0, "Sample id not valid"
    cursor = connector_SQL.cursor()
    # This Query returns tuple list [(1,)] if exist [(0,)]
    cursor.execute(
        "SELECT EXISTS (SELECT `id_sample` FROM tfdb.Sample WHERE `id_sample` LIKE %s) AS success;" % int_id_sample)
    exists = cursor.fetchall()
    query = "SELECT `id_sample`, p.project_name, `Date`, e.name AS Person, "
    query += ", ".join(["`" + column + "`" for column in sql_columns])
    query += """
            FROM `tfdb`.`Sample` AS s
            JOIN `tfdb`.`Project` AS p
            ON s.id_project = p.id_project
            JOIN `tfdb`.`Experimenter` AS e
            ON s.id_experimenter = e.id_experimenter
            WHERE id_sample LIKE %s;
            """ % int_id_sample
    print(query)
    if exists[0][0]:
        try:
            cursor.execute(query)
            sample_data = cursor.fetchall()
        except:
            return 0, str(sys.exc_info())

        return 1, sample_data[0]
    else:
        return 0, "Sample Doesn't exist"


def get_metadata_id_list(connector_SQL, mode, id_sample):
    cursor = connector_SQL.cursor()
    query = f"SELECT id_{mode} FROM tfdb.{mode} WHERE id_sample = {id_sample};"
    cursor.execute(query)
    mode_id_list = cursor.fetchall()
    return mode_id_list


def advanced_search(con_sql, search_keyword, search_type, mode_list, page=1, row_per_page=20, get_total_number=False,
                    search_column_setting_df=None):
    if search_column_setting_df is None:
        select_query = "SELECT DISTINCT s.id_sample, p.project_name as Project, e.name as Person, s.Target_Compo as Composition, s.Deposition_temp as DT, s.Anneal_Temp as AT,\n"


    else:
        search_query_list = []
        for index, row in search_column_setting_df.iterrows():
            search_display = row["search"]
            if search_display:
                search_column = row["SQL_columns"]
                if search_column == "id_project":
                    search_query_list.append("p.project_name as " + search_display)
                elif search_column == "id_experimenter":
                    search_query_list.append("e.name as " + search_display)
                elif search_column == "id_sample":
                    search_query_list.append("DISTINCT s.id_sample as " + search_display)
                else:
                    search_query_list.append("s." + search_column + " as " + search_display)
        select_query = "SELECT "
        for column in search_query_list:
            select_query += column + ", "


    column_query = ""
    mode_list_length = len(mode_list)
    for i in range(mode_list_length):
        mode = mode_list[i]
        column_query += f"(SELECT SUM({mode}.Points) from {mode} Where {mode}.id_sample = s.id_sample) AS {mode}"

        if i < mode_list_length - 1:
            column_query += ",\n"
    from_query = "\nFROM tfdb.Sample AS s\n"
    join_query = "JOIN `tfdb`.`Project` AS p ON s.id_project = p.id_project\nJOIN `tfdb`.`Experimenter` AS e ON s.id_experimenter = e.id_experimenter\n"

    where_query = ""
    if search_type == "ID":
        id_sample_list = [i for i in re.split('[ ,;/]', search_keyword) if i]

        searchable = True
        for id in id_sample_list:
            if not id.isdigit():
                searchable = False
        if searchable:
            where_query = "WHERE " + " OR ".join([f"id_sample = {id}" for id in id_sample_list])
        else:
            where_query = "WHERE id_sample = 0"
    if search_type == "Composition":
        composition_list = search_keyword.split()
        composition_join_list = [
            f" (BINARY(`Target_Compo`) LIKE \"%{compo}\" or BINARY(`Target_Compo`) Like \"%{compo}/%\")\n" for compo in
            composition_list]
        if composition_join_list:
            where_query = "WHERE" + 'AND'.join(composition_join_list)
    if search_type == "Project":
        if search_keyword:
            where_query = f"WHERE p.project_name LIKE \"%{search_keyword}%\""
    if search_type == "Person":
        if search_keyword:
            where_query = f"WHERE e.name LIKE \"%{search_keyword}%\""

    if get_total_number:
        select_query = "SELECT distinct s.id_sample "
        order_query = ";"
        query = select_query + from_query + join_query + where_query + order_query
        cursor = con_sql.cursor()
        cursor.execute(query)
        id_sample_list = [tup[0] for tup in cursor.fetchall()]

        return id_sample_list
    else:
        group_query = "\nGROUP BY s.id_sample"
        order_query = f" ORDER BY s.id_sample DESC LIMIT %d,{row_per_page};" % (row_per_page * (page - 1))
        query = select_query + column_query + from_query + join_query + where_query + group_query + order_query
        search_outcome_df = pd.read_sql(query, con=con_sql)
        search_outcome_df = search_outcome_df.astype(str).replace("None", "").replace("nan", "").replace('(.*)\.0',
                                                                                                         r'\1',
                                                                                                         regex=True)

        return search_outcome_df


def get_file_list(connector_sftp, mode):
    try:
        lst = connector_sftp.listdir(f"/TFDB_drive/{mode}")
        return 1, lst
    except:
        return 0, str(sys.exc_info())


def get_property_metadata_dictionary(connector_SQL, id_sample, mode_list):
    mode_list = ["EDS", "Resistance", "XRD", "Thickness", "Image"]
    total_properties_dictionaries = {}
    for mode in mode_list:
        cursor = connector_SQL.cursor()
        query = "SELECT `id_%s` FROM tfdb.%s WHERE `id_sample` = %d;" % (mode, mode, id_sample)
        try:
            cursor.execute(query)
            metadata_id = cursor.fetchall()
            property_dict = {}
            if len(metadata_id) > 0:
                for i in range(len(metadata_id)):
                    id = metadata_id[i][0]
                    metadata_query = "SELECT * FROM tfdb.%s WHERE `id_%s` = %d;" % (mode, mode, id)
                    cursor.execute(metadata_query)
                    metadata = cursor.fetchall()
                    property_dict[str(id)] = metadata[0]
                total_properties_dictionaries[mode] = property_dict

        except:
            return 0, str(sys.exc_info())

    return 1, total_properties_dictionaries


def update_metadata(connector_SQL, mode, id, meta_list, sql_columns=None):
    meta_list.append(id)
    query = f"UPDATE `tfdb`.`{mode}`\n"
    set_query = "SET "
    set_query += ",\n".join(["`" + column + "` = %s" for column in sql_columns])
    if mode == "Sample":
        where_query = f" WHERE `id_sample` = %s;"
    else:

        where_query = f" WHERE `id_{mode}` = %s;"

    query = query + set_query + where_query
    print(query)
    try:
        cursor = connector_SQL.cursor()
        cursor.execute(query, meta_list)
        connector_SQL.commit()
        return True, ""
    except:
        return False, str(sys.exc_info())


def get_dataframe_from_sftp(con_sftp, file_path):
    with con_sftp.open(file_path) as f:
        df = pd.read_csv(f)
    return df


def get_image_encoded_from_sftp(con_sftp, file_path):
    with con_sftp.open(file_path) as f:
        encoded_string = base64.b64encode(f.read()).decode()
    return encoded_string


def authority_check(connector_SQL):
    project_df = pd.read_sql('SHOW GRANTS FOR CURRENT_USER;', con=connector_SQL)
    project_df.columns = ["Grants"]
    grants = project_df["Grants"][1]
    delete_authority = grants.startswith("GRANT ALL PRIVILEGES") or ("DELETE" in grants)
    insert_authority = grants.startswith("GRANT ALL PRIVILEGES") or ("UPDATE" in grants)
    update_authority = grants.startswith("GRANT ALL PRIVILEGES") or ("UPDATE" in grants)
    return delete_authority, insert_authority, update_authority


def get_metadata_numbers(connector_SQL, mode_list):
    properties = ["Sample"] + mode_list
    cursor = connector_SQL.cursor()
    data_summary_dict = {}
    for property in properties:
        if property == "Sample":
            query = f"SELECT COUNT(*) FROM tfdb.{property};"
            cursor.execute(query)
            count_array = cursor.fetchall()
            count = str(count_array[0][0])
        else:
            query = f"SELECT SUM(`Points`) FROM tfdb.{property};"
            cursor.execute(query)
            point_array = cursor.fetchall()
            count = str(point_array[0][0])

        data_summary_dict[property] = count

    return data_summary_dict


def get_data_from_server(con_sftp, local_directory_path, relative_path):
    remote_file_exists = False
    local_file_exists = False

    try:
        remote_file_stat = con_sftp.stat(relative_path)
        remote_file_size = remote_file_stat.st_size
        remote_file_exists = True
    except:
        pass
    try:
        local_file_stat = os.stat(local_directory_path + relative_path)
        local_file_size = local_file_stat.st_size
        local_file_exists = True
    except:
        pass

    if remote_file_exists:
        if local_file_exists and (remote_file_size == local_file_size):
            return True, "File already exists"
        else:
            try:
                con_sftp.get(relative_path, local_directory_path + relative_path)
            except:
                return False, "Download error."
    else:
        # remote file no exist event
        return False, "Path does not exists."
    return True, ""


def drive_generator(directory_path):
    if os.path.isdir(directory_path):
        pass
    else:
        os.mkdir(directory_path)


def initial_setting(con_sql, local_directory_path, mode_list):
    tfdb_drive_path = local_directory_path + "/TFDB_drive"
    drive_generator(local_directory_path + "/TFDB_drive")
    for mode in mode_list:
        drive_generator(tfdb_drive_path + "/" + mode)

    query = "SELECT property, simple from tfdb_config.metadata_category;"
    cursor = con_sql.cursor()
    # This Query returns tuple list [(1,)] if exist [(0,)]
    cursor.execute(query)
    simple_dict = dict(cursor.fetchall())
    return simple_dict


def get_existing_property_metadata_id_list(con_sql, id_sample, mode_list):
    id_list_dict = {}
    for mode in mode_list:
        query = f"SELECT id_{mode} from {mode} WHERE id_sample = {id_sample};"
        print(query)
        cursor = con_sql.cursor()
        # This Query returns tuple list [(1,)] if exist [(0,)]
        cursor.execute(query)
        id_list = [tup[0] for tup in cursor.fetchall()]
        id_list_dict[mode] = id_list
    return id_list_dict


def download_data_with_sample_id_and_property_id(con_sftp, local_directory_path, mode, simple, id_sample, id_property):
    tag = f"/TFDB_drive/{mode}/{id_sample}-{id_property}"
    if simple:
        relative_path = tag + ".csv"
        success, error_message = get_data_from_server(con_sftp, local_directory_path, relative_path)

    else:
        try:
            file_list = con_sftp.listdir(path=tag)
        except:
            return False, "Path does not exists."

        if file_list:
            drive_generator(local_directory_path + tag)
            for file in file_list:
                relative_path = tag + "/" + file
                success, error_message = get_data_from_server(con_sftp, local_directory_path, relative_path)

    return success, error_message




if __name__ == '__main__':
    pass
