import pandas as pd
import plotly.express as px
import plotly
import plotly.graph_objects as go
import os
import base64
from PIL import Image
import io
import re
import numpy as np


def visualize_data(mode, directory, file_list, simple, xy_coordinate):
    """
    This function visualize the data that user selected to upload each properties has it's own format, (simple data
    such as EDS, Thickness, Resistance, are .csv file.) if they are in right format, the function returns the plot (
    library: plotly, html format) and number of measured points count.
    """
    html = '<html><head><body><p>Wrong file format</p></body></html>'
    right_format, response = file_format_check(mode, directory, file_list, simple, xy_coordinate)
    points_counter = None
    if mode == 'EDS':
        df = pd.read_csv(directory + "/" + file_list[0])
        if right_format:
            html = show_EDS_df(df)
            points_counter = response
    elif mode == 'Image':
        html = ""
        points_counter = response
    elif mode == 'Resistance':
        df = pd.read_csv(directory + "/" + file_list[0]).round(decimals=5)
        if right_format:
            html = show_Resistance_df(df)
            points_counter = response
    elif mode == 'XRD':
        if right_format:
            html = show_XRD_file_list(file_list)
            points_counter = response
    elif mode == 'Hardness':
        if right_format:
            html = show_Hardness_file_list(file_list)
            points_counter = response
    elif mode == 'Thickness':
        df = pd.read_csv(directory + "/" + file_list[0])
        if right_format:
            html = show_Thickness_df(df)
            points_counter = response
    else:
        html = '<html><head><body><p>Plot for this property is not ready</p></body></html>'
    return html, points_counter

def visualize_remote_data_to_plot(con_SFTP, remote_path, mode):
    """
        The input file to each plot: SFTPFile class.
        see https://docs.paramiko.org/en/stable/api/sftp.html?highlight=SFTPFile#paramiko.sftp_file.SFTPFile

    """

    html = '<html><head><body><p>Plot for this property is not ready</p></body></html>'
    try:
        con_SFTP.stat(remote_path)
    except IOError:
        return '<html><head><body><p>data file does not exist</p></body></html>'

    if mode == 'EDS':
        with con_SFTP.open(remote_path, "r") as f:
            df = pd.read_csv(f)
        html = show_EDS_df(df)
    elif mode == 'Resistance':
        with con_SFTP.open(remote_path, "r") as f:
            df = pd.read_csv(f).round(decimals=5)
        html = show_Resistance_df(df)
    elif mode == 'Image':
        with con_SFTP.open(remote_path, "r") as f:
            tif_format = remote_path.lower().endswith((".tif", ".tiff"))
            html = show_photo_img(f, tif_format=tif_format)
    elif mode == 'XRD':
        with con_SFTP.open(remote_path, "r") as f:
            html = show_XRD_plot_form_file(f)
    elif mode == 'Hardness':
        pass
    elif mode == 'Thickness':
        with con_SFTP.open(remote_path, "r") as f:
            df = pd.read_csv(f)
        html = show_Thickness_df(df)
    return html



def fig_to_html(fig):
    config={'doubleClick': 'reset', 'displaylogo': False, 'scrollZoom': True}
    html = '<html><head><meta charset="utf-8"/><script src="https://cdn.plot.ly/plotly-latest.min.js"></script></head><body>'
    html += plotly.offline.plot(fig, output_type='div', include_plotlyjs='cdn', config = config)
    html += '</body></html>'
    del fig
    return html


def show_fig():
    fig = go.Figure()
    return fig_to_html(fig)


def show_EDS_df(df):
    fig = go.Figure()
    compo = df.columns[2::]
    for comp in compo:
        fig.add_trace(go.Scatter(visible=False,
                                 x = df['X'], y = df['Y'], mode='markers', text = df[comp] ,name= comp + '(%)',
                                 marker=dict(symbol='square', size = 7, color=df[comp], colorscale='Jet', showscale=True)))
    fig.data[0].visible = True
    steps = []
    for i in range(len(fig.data)):
        step = dict(
            method="update",
            args=[{"visible": [False] * len(fig.data)}],  # layout attribute
            label = compo[i]
        )
        step["args"][0]["visible"][i] = True  # Toggle i'th trace to "visible"
        steps.append(step)

    sliders = [dict(
        active=0,
        steps=steps
    )]
    fig.update_yaxes(scaleanchor="x", scaleratio=1)
    fig.update_layout(
        title="EDS Composition (%)",
        xaxis_title="X (mm)",
        yaxis_title="Y (mm)",
        template = 'plotly_white',
        sliders=sliders
    )
    return fig_to_html(fig)


def show_Resistance_df(df):
    fig = go.Figure(data = go.Scatter(visible=True,
             x=df['X'], y=df['Y'], mode='markers', text=df["Resistance"], name="Resistance",
             marker=dict(symbol='square', size=7, color=df["Resistance"], colorscale='Jet',
                         showscale=True)))


    fig.update_layout(
        title="Resistance (Ω)",
        xaxis_title="X (mm)",
        yaxis_title="Y (mm)",
        template='plotly_white'
    )
    pd.options.display.float_format = '{:.2f}'.format
    fig.update_yaxes(scaleanchor="x", scaleratio=1)
    return fig_to_html(fig)


def show_Thickness_df(df):
    fig = go.Figure(data = go.Scatter(visible=True,
             x=df['X'], y=df['Y'], mode='markers', text=df["Thickness"], name="Thickness",
             marker=dict(symbol='square', size=7, color=df["Thickness"], colorscale='Jet',
                         showscale=True)))


    fig.update_layout(
        title="Thickness (nm)",
        xaxis_title="X (mm)",
        yaxis_title="Y (mm)",
        template='plotly_white'
    )
    pd.options.display.float_format = '{:.2f}'.format
    fig.update_yaxes(scaleanchor="x", scaleratio=1)
    return fig_to_html(fig)


def show_XRD_axis_df(df):
    unique_points_df = df.groupby(['X', 'Y']).size().reset_index().rename(columns={0: 'count'})
    fig = px.scatter(unique_points_df, x="X", y="Y")

    fig.update_traces(marker=dict(size=12, symbol='square'))
    fig.update_yaxes(scaleanchor="x", scaleratio=1)
    fig.update_layout(
        xaxis_title="X (mm)",
        yaxis_title="Y (mm)",
        template='plotly_white'
    )
    return fig_to_html(fig)


def show_XRD_plot_form_file(f):
    condition = f.readline()
    df = pd.read_csv(f, delim_whitespace=True, names=["2Theta", "peak"])
    if len(df) > 0:
        fig = px.line(df, x="2Theta", y="peak")
        fig.update_layout(
            title="XRD peak",
            xaxis_title="2Theta (°)",
            yaxis_title="peak",
            template='plotly_white'
        )
        return fig_to_html(fig)
    else:
        return "No data"

    return fig_to_html(fig)


def show_XRD_file_list(file_list):
    lst = []
    for file_name in file_list:

        append_list = re.split("\(|,|\)", file_name.replace(" ", ""))
        x = float(append_list[1])
        y = float(append_list[2])
        lst.append([x, y, file_name])

    df = pd.DataFrame(lst, columns=['X', 'Y', 'File name'])
    fig = go.Figure(data=go.Scatter(visible=True,
            x=df['X'], y=df['Y'], mode='markers', text=df['File name'], name="Measured points",
            marker=dict(symbol='square', size=7)))
    fig.update_layout(
        xaxis_title="X (mm)",
        yaxis_title="Y (mm)",
        template='plotly_white'
    )
    pd.options.display.float_format = '{:.2f}'.format
    fig.update_yaxes(scaleanchor="x", scaleratio=1)
    return fig_to_html(fig)


def show_photo_img(file,tif_format=False):


    # Constants

    if tif_format:
        a = file.read()
        encoded_string = base64.b64encode(a).decode()

        im = Image.open(io.BytesIO(base64.b64decode(encoded_string)))
        buffered = io.BytesIO()
        out = im.convert("RGB")
        out.save(buffered, format="JPEG")

        encoded_string = base64.b64encode(buffered.getvalue()).decode()
        del out, buffered
    else:
        a = file.read()
        encoded_string = base64.b64encode(a).decode()

        im = Image.open(io.BytesIO(base64.b64decode(encoded_string)))
    img_width = 1600
    img_height = 1600
    scale_factor = 0.5

    img_width, img_height = im.size
    del im
    # Add the prefix that plotly will want when using the string as source

    encoded_string = "data:image/png;base64," + encoded_string
    # Add invisible scatter trace.
    # This trace is added to help the autoresize logic work.
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=[0, img_width * scale_factor],
            y=[0, img_height * scale_factor],
            mode="markers",
            marker_opacity=0,
            hoverinfo='skip'
        )
    )

    # Configure axes
    fig.update_xaxes(
        visible=False,
        range=[0, img_width * scale_factor]
    )

    fig.update_yaxes(
        visible=False,
        range=[0, img_height * scale_factor],
        # the scaleanchor attribute ensures that the aspect ratio stays constant
        scaleanchor="x"
    )

    # Add image
    fig.add_layout_image(
        dict(
            x=0,
            sizex=img_width * scale_factor,
            y=img_height * scale_factor,
            sizey=img_height * scale_factor,
            xref="x",
            yref="y",
            opacity=1.0,
            layer="below",
            sizing="stretch",
            source=encoded_string)
    )

    fig.update_layout(
        template='plotly_white'
    )
    # Configure other layout
    fig.update_layout(
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
    )
    encoded_string = None
    # Disable the autosize on double click because it adds unwanted margins around the image
    # More detail: https://plotly.com/python/configuration-options/
    return fig_to_html(fig)


def show_Hardness_file_list(file_list):
    lst = []
    for file_name in file_list:

        append_list = re.split("\(|,|\)", file_name.replace(" ", ""))
        x = float(append_list[1])
        y = float(append_list[2])
        lst.append([x, y, file_name])

    df = pd.DataFrame(lst, columns=['X', 'Y', 'File name'])
    fig = go.Figure(data=go.Scatter(visible=True,
            x=df['X'], y=df['Y'], mode='markers', text=df['File name'], name="Measured points",
            marker=dict(symbol='square', size=7)))
    fig.update_layout(
        xaxis_title="X (mm)",
        yaxis_title="Y (mm)",
        template='plotly_white'
    )
    pd.options.display.float_format = '{:.2f}'.format
    fig.update_yaxes(scaleanchor="x", scaleratio=1)
    return fig_to_html(fig)




def has_duplicates(seq):
    """This function checks if there is duplicate in 2-dimension list"""
    seen = []
    unique_list = [x for x in seq if x not in seen and not seen.append(x)]
    return len(seq) != len(unique_list)


def is_number_repl_isdigit(s):
    """ Returns True is string is a number. """
    return s.lstrip('-').replace('.','',1).isdigit()



def file_format_check(mode, directory, file_list,simple, xy_coordinate):
    """
    This Function checks validity of the file.
    mode:
        EDS: full_path = (*.csv)
            columns:[X, Y, *(chemical_symbol)] ex)[X, Y, Zr, Fe, Cr, Ti, Al]
        Resistance: full_path = (*.csv)
            columns: [X, Y, Resistance]
        XRD: full_path = (directory)
            not decided.
        Image: full_path = (*.png *.jpg *.bmp *.jpeg)
            return True whatever (maybe, file size limit)

    retun: boolean
    """


    if directory == "" or file_list == []:
        error_message = "Please select the file"
        return False, error_message

    if simple:
        if mode == 'EDS':
            full_path = directory + "/" + file_list[0]
            df = pd.read_csv(full_path)
            if list(df.columns)[0:2] == ['X', 'Y'] and list(df.columns)[2] != 'Resistance' and list(df.columns)[
                2] != 'Thickness':
                row_num = df["X"].count()
                if row_num >= 1:
                    return True, row_num
            else:
                return False, "Columns format needs to be X, Y, Components"

        elif mode == 'Resistance':
            full_path = directory + "/" + file_list[0]
            df = pd.read_csv(full_path).round(decimals=5)
            if list(df.columns) == ['X', 'Y', 'Resistance']:
                row_num = df["Resistance"].count()
                if row_num >= 1:
                    return True, row_num
            else:
                return False, "Columns format needs to be X, Y, Resistance"

        elif mode == 'Thickness':
            full_path = directory + "/" + file_list[0]
            df = pd.read_csv(full_path).round(decimals=5)
            if list(df.columns) == ['X', 'Y', 'Thickness']:
                row_num = df["Thickness"].count()
                if row_num >= 1:
                    return True, row_num
            else:
                return False, "Columns format needs to be X, Y, Thickness"
        else:
            full_path = directory + "/" + file_list[0]
            df = pd.read_csv(full_path).round(decimals=5)
            row_num = len(df)
            if row_num >= 1:
                return True, row_num
    else:
        if xy_coordinate:
            r = re.compile('\(.+,.+\).*\..+')
            if len(file_list) < 1:
                return False, "No file exists in the selected directory"
            x_y_list = []
            for file_name in file_list:
                match = r.match(file_name)
                if match == None:
                    return False, "the file name must be  \n(X,Y)filename"
                lst = re.split("\(|,|\)", file_name.replace(" ", ""))
                x = lst[1]
                y = lst[2]
                x_y_list.append([x, y])
                if not (is_number_repl_isdigit(x) and is_number_repl_isdigit(y)):
                    return False, "Coordinate values must be number"
            if has_duplicates(x_y_list):
                return False, "There is duplicated points in the list"
            return True, len(x_y_list)
        else:
            return True, len(file_list)



    error_message = "Wrong file format. Please check the data."
    return False, error_message