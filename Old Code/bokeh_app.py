from bokeh.io import curdoc
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Select
from bokeh.plotting import figure
import pandas as pd
import os

# Set working directory
path = '/Users/mau/Library/CloudStorage/Dropbox/Mac/Documents/Dissertation/Chapter 2/Data'
os.chdir(path)

# Load data into a DataFrame
df40_match4_12 = pd.read_parquet("df40_match4_12.parquet")

# Keep only data that has percent_return > 0 and percent_return < 100
df40_match4_12 = df40_match4_12[(df40_match4_12['percent_return'] > 0) & (df40_match4_12['percent_return'] < 100)]

# Create a list of unique values from the 'playerkey' column of your data:
players_match4_12 = df40_match4_12['playerkey'].unique()

# Create a ColumnDataSource object that will store your data
source = ColumnDataSource(data=df40_match4_12)

# Create a figure object and add a line glyph for your data:
p = figure(x_axis_label='time', y_axis_label='percent_return')
line = p.scatter('time', 'percent_return', source=source)

# Create a Select widget with a list of unique values from the 'playerkey' column of your data:
player_select = Select(title="Player", value="all", options=['all']+players_match4_12.astype(str).tolist())

# Define a callback function for the Select widget that filters your data based on the selected player and updates the ColumnDataSource object:
def update(attr, old, new):
    if player_select.value == 'all':
        source.data = df40_match4_12
    else:
        source.data = df40_match4_12[df40_match4_12['playerkey'] == int(player_select.value)]

# Add the callback function to the Select widget:
player_select.on_change('value', update)

# Add the plot and widget to the document:
curdoc().add_root(column(player_select, p))

# bokeh serve --show /Users/mau/Library/CloudStorage/Dropbox/Mac/Documents/GitHub/Gamblers_Investors/bokeh_app.py