# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import time
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
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
while df.empty:
    df = pd.DataFrame(session.execute('SELECT * FROM champ'))
    print("No data received yet...")
    time.sleep(60)
print(df.head())
#df['champion'] = pd.to_numeric(df['champion'])
champs = df[df["champion"] != '-1']
banChart = px.bar(champs.sort_values(by="count", ascending=False).head(15), x="champion", y="count")
banChart.update_xaxes(type='category')
banChart.update_xaxes(categoryorder='total ascending')
banChart.update_layout(margin_t=5)

df = pd.DataFrame(session.execute('SELECT * FROM stats'))
print(df.head())
total_length = df["duration"].sum()
total_matches = df["tot_matches"].sum()
indicator = go.Figure(go.Indicator(
    mode = "number+delta",
    value = total_length/total_matches/60,
    number = {'suffix': " min"},
    delta = {'position': "top", 'reference': 25},
    domain = {'x': [0, 1], 'y': [0, 1]}))
indicator.update_layout(margin_t=0)

df = pd.DataFrame(session.execute('SELECT * FROM stats'))
red_wins = df["red_win"].sum()
blue_wins = total_matches - red_wins
winDf = pd.DataFrame({'Side':['Red Side', 'Blue Side'], 'Data':[red_wins, blue_wins]})
pie = px.pie(winDf, values='Data', names='Side', color='Side', color_discrete_map={'Red Side':'red','Blue Side':'blue'})
pie.update_layout(margin_t=15)

app.layout = html.Div([
    html.H1(children='League of Legends Stats Live Dashboard', style={"marginRight":"auto", "marginLeft":"auto", "textAlign": "center"}),
    html.Br(),
    html.Div("Most banned champions", style={"marginRight":"auto", "marginLeft":"auto", "textAlign": "center", "fontSize": "20px"}),
    html.Div([dcc.Graph(id="ban", figure = banChart)], className="row"),
    html.Br(),
    html.Div([
            html.Div("Average game length",
                className="six columns", style={"marginRight":"auto", "marginLeft":"auto", "textAlign": "center", "fontSize": "20px"}),
            html.Div("WinRate by side",
                className="six columns", style={"marginRight":"auto", "marginLeft":"auto", "textAlign": "center", "fontSize": "20px"})
            ], className="row"),

    html.Div([
        html.Div([
            dcc.Graph(id='game', figure = indicator)],
            className="six columns"),
        html.Div([
            dcc.Graph(id='wins', figure = pie)],
            className="six columns")
        ], className="row"),

    html.Div([dcc.Interval(id = "interval", interval = 300000, n_intervals = 0)])

], style={"maxWidth":"94vw", "marginRight":"auto", "marginLeft":"auto"})

@app.callback(
    [Output("ban", "figure"), Output("game", "figure"), Output("wins", "figure")],
    [Input("interval", "n_intervals")]
)
def update(n_intervals):
    cluster = Cluster()
    session = cluster.connect('riot')
    session.row_factory = dict_factory

    df = pd.DataFrame(session.execute('SELECT * FROM champ'))
    print(df.head())
    #df['champion'] = pd.to_numeric(df['champion'])
    champs = df[df["champion"] != '-1']
    banChart = px.bar(champs.sort_values(by="count", ascending=False).head(15), x="champion", y="count")
    banChart.update_xaxes(type='category')
    banChart.update_xaxes(categoryorder='total ascending')
    banChart.update_layout(margin_t=5)

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
    indicator.update_layout(margin_t=0)

    df = pd.DataFrame(session.execute('SELECT * FROM stats'))
    red_wins = df["red_win"].sum()
    blue_wins = total_matches - red_wins
    winDf = pd.DataFrame({'Side':['Red Side', 'Blue Side'], 'Data':[red_wins, blue_wins]})
    pie = px.pie(winDf, values='Data', names='Side', color='Side', color_discrete_map={'Red Side':'red','Blue Side':'blue'})
    pie.update_layout(margin_t=15)

    return banChart, indicator, pie


if __name__ == '__main__':
    app.run_server(debug=True)