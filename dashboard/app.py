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
banChart = px.bar(champs, x="champion", y="count")
banChart.update_xaxes(type='category')
banChart.update_xaxes(categoryorder='total ascending')

df = pd.DataFrame(session.execute('SELECT * FROM stats'))
print(df.head())
total_length = df["duration"].sum()
total_matches = df["tot_matches"].sum()
indicator = go.Figure(go.Indicator(
    mode = "number+delta",
    value = total_length/total_matches/60,
    number = {'suffix': "min"},
    delta = {'position': "top", 'reference': 25},
    domain = {'x': [0, 1], 'y': [0, 1]}))

df = pd.DataFrame(session.execute('SELECT * FROM stats'))
red_wins = df["red_win"].sum()
blue_wins = total_matches - red_wins
winDf = pd.DataFrame({'Side':['Red Side', 'Blue Side'], 'Data':[red_wins, blue_wins]})
pie = px.pie(winDf, values='Data', names='Side', color='Side', color_discrete_map={'Red Side':'light_red','Blue Side':'light_blue'})

app.layout = html.Div([
    html.H1(children='League of Legends Stats Live Dashboard'),
    dcc.Graph(id='Champion\'s ban ', figure=banChart),
    html.Div([
        html.Div([
            dcc.Graph(id='Game Duration', figure=indicator)],
            style = {'column-count': 3, 'width': '50%'}),
        html.Div([
            dcc.Graph(id='Wins by side', figure=pie)],
            style = {'column-count': 3, 'width': '50%'})
        ])

])

if __name__ == '__main__':
    app.run_server(debug=True)