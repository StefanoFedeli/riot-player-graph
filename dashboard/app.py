# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# assume you have a "long-form" data frame
# see https://plotly.com/python/px-arguments/ for more options
cluster = Cluster()
session = cluster.connect('riot')
session.row_factory = dict_factory

df = pd.DataFrame(session.execute('SELECT * FROM champ'))
print(df.head())
#df['champion'] = pd.to_numeric(df['champion'])
champs = df[df["champion"] != '-1']
fig = px.bar(champs, x="champion", y="count")
fig.update_xaxes(type='category')
fig.update_xaxes(categoryorder='total ascending')

df = pd.DataFrame(session.execute('SELECT * FROM stats'))
print(df)
df['duration'] = df['duration'] / df['tot_matches']

timeline = go.Figure(data=go.Scatter(x=df['slot'], y=df['duration']))

df = pd.DataFrame(session.execute('SELECT * FROM stats'))
total_length = df["duration"].sum()
total_matches = df["tot_matches"].sum()
indicator = go.Figure(go.Indicator(
    mode = "number+delta",
    value = total_length/total_matches/60,
    number = {'suffix': "min"},
    delta = {'position': "top", 'reference': 25},
    domain = {'x': [0, 1], 'y': [0, 1]}))
indicator.update_layout(paper_bgcolor = "lightgray")

app.layout = html.Div(children=[
    html.H1(children='League of Legends'),

    dcc.Graph(
        id='Champion\'s ban ',
        figure=fig
    ),

    dcc.Graph(
        id='duration Game',
        figure=timeline
    ),

    dcc.Graph(
            id='duration Game2',
            figure=indicator
        )
])

if __name__ == '__main__':
    app.run_server(debug=True)