""" Plotly Dash Report Summary of the SEC RSS Database instance
"""


import os, calendar, math, socket, threading, logging, gettext
from dateutil import parser
import numpy as np
import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, ClientsideFunction, State
import plotly.express as px
from datetime import date, datetime

try:
    from arellepy.CntlrPy import CntlrPy
except:
    from plugin.arellepy.CntlrPy import CntlrPy
    
from .RssDB import rssDBConnection


class RssDBDash:
    def __init__(self, xconn, fromDate=None, toDate=None):
        now_ = datetime.today().date()
        # Create a connection for the report
        xconn.cntlr.addToLog(_('Creating New Connection for DB report'), messageCode="RssDB.Info", file="",  level=logging.INFO)
        xconn.cntlr.showStatus(_('Creating New Connection for DB report'))
        setConfigDir = os.path.dirname(xconn.cntlr.userAppDir)
        targetResDir = os.path.dirname(xconn.cntlr.configDir)
        a = CntlrPy(
            instConfigDir=setConfigDir,
            useResDir=targetResDir,
            logFileName="logToBuffer"
        )
        conn = rssDBConnection(a, **{k:v for k,v in xconn.conParams.items() if not k =='cntlr'})

        self.conn = conn
        self.dbStats = conn.getDbStats()['dictResult']
        # validate Dates
        for k,v in {'From': fromDate, 'To': toDate}.items():
            if v:
                try:
                    datetime.strptime(v, '%Y-%m-%d')
                except:
                    self.conn.cntlr.addToLog(_('{} Date is not in the correct fromat, date should be in the format yyyy-mm-dd').format(k),
                                                messageCode="RssDB.Error", file="",  level=logging.ERROR)
                    self.conn.cntlr.addToLog(_('Reverting to default dates to initialize dashboard'),
                                                messageCode="RssDB.Info", file="",  level=logging.INFO)
                    fromDate = None
                    toDate = None
        
        if (fromDate and toDate) and (datetime.strptime(toDate, '%Y-%m-%d') <= datetime.strptime(fromDate, '%Y-%m-%d')):
            self.conn.cntlr.addToLog(_('To Date must be later than From date'), messageCode="RssDB.Info", file="",  level=logging.INFO)
            self.conn.cntlr.addToLog(_('Reverting to default dates to initialize dashboard'),
                                        messageCode="RssDB.Info", file="",  level=logging.INFO)
            fromDate = None
            toDate = None

        if not fromDate and not toDate:
            lastFiling = self.dbStats.get('LatestFiling', None)
            if lastFiling:
                lastFilingYear = parser.parse(lastFiling).date().year if isinstance(lastFiling, str) else lastFiling.year
                fromDate = str(date(lastFilingYear-2, 1, 1)) # last 3 years
        
        # Allowable Date range for date picker
        # date of first XBRL filing is in 2005, thats a constant
        self.minDate = date(2005,1,1)
        # End of current year if not available (empty database)
        self.maxDate = date(now_.year,12,31)

        self.date_picker_start_date = fromDate if fromDate else date(now_.year-2,1,1) # just the last 3 years
        self.date_picker_end_date = toDate if toDate else date(now_.year,12,31)

        self.mapbox_access_token = 'pk.eyJ1Ijoic2VsZ2FtYWwiLCJhIjoiY2tmd2FoMGkzMDBmNDJ1cWYwdGUxZm8ydCJ9.eGKld0cSO6zGvDMzfGAafA'
        px.set_mapbox_access_token(self.mapbox_access_token)
        self.dbInfo = '{}: {}'.format(conn.product, os.path.basename(conn.conParams['database']) if conn.product=='sqlite' else conn.conParams['host'] + '/' + conn.conParams['database']) 
        if conn.product == 'postgres':
            self.dbInfo += ' - schema: {}'.format(conn.schema)
        
        self.dbInfo += ' ({})'.format(self.dbStats['DatabaseSize'])
        # place holder to initialize the app without having to query db
        self.filingsSummary = None
        self.filersLocations = None
        # initialized for the select all conflict
        self._formTypes = ['10-Q', '10-K']
        self.formTypes = [{'label': x, 'value': x}  for x in self._formTypes]
        self._sicDiv = ['Manufacturing', 'Finance, Insurance, and Real Estate', 'Services', 'Not Assigned']
        self.sicDiv = [{'label': x, 'value': x}  for x in self._sicDiv]
        self.years_range = None
        self.years = None
        # populate variables
        # self.getData(1, fromDate=str(self.date_picker_start_date), toDate=str(self.date_picker_end_date), returnRes=False)

        # dash
        self.app = dash.Dash(__name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}])
        self.app.title = os.path.basename(conn.conParams['database'])
        self.server = self.app.server
        self.app.layout = self.appLayout()

        self.app.clientside_callback(
            ClientsideFunction(namespace="clientside", function_name="resize"),
            Output("output-clientside", "children"),
            [Input("count_graph", "figure")],
        )

        self.app.callback(
            [
                Output("form_types", "value")
            ],
            [
                Input("form_types_select_all", "value")

            ]
        )(self.selectAllForms)

        self.app.callback(
            [
                Output("sic_divisions", "value")
            ],
            [
                Input("sic_divisions_select_all", "value")

            ]
        )(self.selectAllDivisions)

        self.app.callback(
            [Output("dummy", "children"),
                Output("year_slider", "min"),
                Output("year_slider", "max"),
                Output("year_slider", "value"),
                Output("year_slider", "marks"),
                Output("form_types", "options"),
                Output("sic_divisions", "options"),
                Output("feeds_text", "children"),
                Output("filingsText", "children"),
                Output("filesText", "children"),
                Output("filersText", "children"),
                Output("lastUpdateText", "children"),            
                Output("form_types_select_all", "value"),
                Output("sic_divisions_select_all", "value")
            ],
            [
                Input("refresh-button", 'n_clicks')
            ],
            [
                State("date-picker-range", 'start_date'),
                State("date-picker-range", 'end_date')
            ]        
        )(self.getData)

        self.app.callback(
            [
                Output("feedsText_slctd", "children"),
                Output("filingsText_slctd", "children"),
                Output("formsText_slctd", "children"),
                Output("industryText_slctd", "children"),
                Output("filerText_slctd", "children"),
            ],
                [
                Input("form_types", "value"),
                Input("sic_divisions", "value"),
                Input("year_slider", "value"),
            ]
        )(self.update_boxes)

        self.app.callback(
            [Output("count_graph", "figure"),
            Output("forms_graph", "figure"),
            Output("industry_graph", "figure"),
            Output("map_graph", "figure")],
            [
                Input("form_types", "value"),
                Input("sic_divisions", "value"),
                Input("year_slider", "value"),
                Input("time_freq_selector", "value"),
            ],
        )(self.make_figures)

    def selectAllForms(self, slctAll):
        res = ''
        if slctAll:
            res = self._formTypes
        return (res,)

    def selectAllDivisions(self, slctAll):
        res = ''
        if slctAll:
            res = self._sicDiv
        return (res,)


    def getData(self, n, fromDate=None, toDate=None, returnRes=True):
        gettext.install('arelle')
        newConn = False
        xconn = self.conn
        conn = None
        if xconn.product in ('sqlite', 'mongodb'):
            xconn.cntlr.addToLog(_('Creating New Connection for {} in new thread/process').format(xconn.product), messageCode="RssDB.Info", file="",  level=logging.INFO)
            xconn.cntlr.showStatus(_('Creating New Connection for {} in new thread/process').format(xconn.product))
            setConfigDir = os.path.dirname(xconn.cntlr.userAppDir)
            targetResDir = os.path.dirname(xconn.cntlr.configDir)
            a = CntlrPy(
                instConfigDir=setConfigDir,
                useResDir=targetResDir,
                logFileName="logToBuffer"
            )
            conn = rssDBConnection(a, **{k:v for k,v in xconn.conParams.items() if not k =='cntlr'})
            newConn = True
        else:
            conn = self.conn
        
        stats, filingsSummaryDict, industrySummaryDict, filersLocationsDict = conn.getReportData(fromDate=fromDate, toDate=toDate)
        if conn.product == 'sqlite' and newConn:
            conn.close()
        monthsQ = [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]
        eoq = [3, 3, 3, 6, 6, 6, 9, 9, 9, 12, 12, 12]
        df = pd.DataFrame.from_dict(filingsSummaryDict)
        df['feedMonth'] = pd.to_datetime(df['feedMonth'])
        df['year'] = df['feedMonth'].dt.year
        df['month'] = df['feedMonth'].dt.strftime('%b-%Y')
        df['endOfQuarter'] = [datetime(x.year, eoq[x.month - 1], calendar.monthrange(
            x.year, eoq[x.month - 1])[-1]).date() for x in df.feedMonth]
        df['quarter'] = ['Q' + str(monthsQ[x.month - 1]) +
                        '-' + str(x.year) for x in df.feedMonth]
        df['division_name'] = [industrySummaryDict[str(x)]['division_name'] if industrySummaryDict.get(
            str(x), None) else 'Not Assigned' for x in df.assignedSic]

        _formTypes_sort = list(df.groupby(['formType'])['count'].sum().reset_index().sort_values(by='count',ascending=False).formType)
        _formTypes = []
        firstFewForms = ('10-q', '10-q/a', '10-k', '10-k/a')
        for x in _formTypes_sort:
            if x.lower() in firstFewForms:
                _formTypes.append(x)

        for x in _formTypes_sort:
            if not x.lower() in firstFewForms:
                _formTypes.append(x)

        if not len(list(set(df.formType))) == len(_formTypes):
            raise Exception("Something is wrong with forms!!")
        # _formTypes = list(set(df.formType))
        formTypes = [{"label": x, "value": x} for x in _formTypes]

        _sicDiv = list(df.groupby(['division_name'])['count'].sum().reset_index().sort_values(by='count',ascending=False).division_name)
        # _sicDiv = list(set(df.division_name))
        sicDiv = [{"label": x, "value": x} for x in _sicDiv]

        years_range = list(np.sort(df.year.unique()))
        years = {str(min(years_range)): {'label': str(min(years_range))},
                str(years_range[int(len(years_range) * .3)]): {'label': str(years_range[int(len(years_range) * .3)])},
                str(years_range[int(len(years_range) * .6)]): {'label': str(years_range[int(len(years_range) * .6)])},
                str(max(years_range)): {'label': str(max(years_range))}}

        filers_df = pd.DataFrame.from_dict(filersLocationsDict)
        self.dbStats = stats
        self.filingsSummary = df
        self.filersLocations = filers_df
        self._formTypes = _formTypes
        self.formTypes = formTypes
        self._sicDiv = _sicDiv
        self.sicDiv = sicDiv
        self.years = years
        self.years_range = years_range
        if returnRes:
            return ([''],
                    min(df.year), 
                    max(df.year), 
                    [max(df.year) - 10, max(df.year)], 
                    years, 
                    formTypes, 
                    sicDiv,
                    self.human_format(stats['CountFeeds']), 
                    self.human_format(stats['CountFilings']), 
                    self.human_format(stats['CountFiles']), 
                    self.human_format(stats['CountFilers']), 
                    str(stats['LastUpdate']),
                    0, 0, # select all false returns all (if non selected returns all data)                    
                    )

    def human_format(self, num):
        num = int(num)    
        if num == 0:
            return "0"
        magnitude = int(math.log(num, 1000))
        mantissa = str(int(num / (1000 ** magnitude)))
        return mantissa + ["", "K", "M", "G", "T", "P"][magnitude]

    def appLayout(self):
        layout = html.Div(
            [
                dcc.Store(id="filingsDF"),
                # empty Div to trigger javascript file for graph resizing
                html.Div(id="output-clientside"),
                html.Div(id="dummy"),
                html.Div(id="dummy2"),
                html.Div(id="dummy3"),
                html.P(["This report is based on plotly Dash gallery example found ",
                        html.A("here ",
                               href="https://dash-gallery.plotly.host/dash-oil-and-gas/", target="_blank"), "and ",
                        html.A("here.", href="https://github.com/plotly/dash-sample-apps/tree/master/apps/dash-oil-and-gas", target="_blank")], className="ref-note"),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Img(
                                    src=self.app.get_asset_url(
                                        "dash-logo.png"),
                                    id="plotly-image",
                                    style={
                                        "height": "60px",
                                        "width": "auto",
                                        "margin-bottom": "25px",
                                    },
                                )
                            ],
                            className="one-third column",
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.H3(
                                            "SEC XBRL Filings RSS Feed Summary",
                                            style={"margin-bottom": "0px"},
                                        ),
                                        html.H5(
                                            "Database Overview", style={"margin-top": "0px"}
                                        ),
                                    ]
                                )
                            ],
                            className="one-half column",
                            id="title",
                        ),
                        html.Div(
                            [
                                html.A(
                                    html.Button(
                                        "Learn More", id="learn-more-button"),
                                    href="https://plot.ly/dash/pricing/",
                                )
                            ],
                            className="one-third column",
                            id="button",
                        ),
                    ],
                    id="header",
                    className="row flex-display",
                    style={"margin-bottom": "25px"},
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.P(
                                    "Select date range and click Refresh Data:",
                                    className="control_label"
                                ),
                                dcc.DatePickerRange(
                                    id='date-picker-range',
                                    min_date_allowed = self.minDate,
                                    max_date_allowed = self.maxDate,
                                    start_date = self.date_picker_start_date,
                                    end_date = self.date_picker_end_date
                                ),

                                html.Button("Refresh Data From DB",
                                            id="refresh-button", className=""),

                                html.P(
                                    "Filter By years:",
                                    className="control_label",
                                ),
                                dcc.RangeSlider(
                                    id="year_slider",
                                    # min=min(self.filingsSummary.year),
                                    # max=max(self.filingsSummary.year),
                                    # value=[max(self.filingsSummary.year)-10,
                                    #        max(self.filingsSummary.year)],
                                    className="dcc_control",
                                    # marks=self.years,

                                ),
                                html.P("Time axis:", className="control_label"),
                                dcc.RadioItems(
                                    id="time_freq_selector",
                                    options=[
                                        {"label": "Yearly ", "value": "year"},
                                        {"label": "Quarterly ",
                                            "value": "quarter"},
                                        {"label": "Monthly ", "value": "month"},
                                    ],
                                    value="quarter",
                                    labelStyle={"display": "inline-block"},
                                    className="dcc_control",
                                ),
                                html.P("Select Form Type(s):",
                                       className="control_label"),
                                dcc.RadioItems(
                                    id="form_types_select_all",
                                    options=[{'label': 'Select All', 'value':1}, {'label': 'Select None', 'value':0}],
                                    value=0,
                                    className="dcc_control",
                                    style={'display': 'grid', 'font-size':'0.7em', 'grid-template-columns': 'auto auto auto auto'}
                                ),
                                dcc.Checklist(
                                    id="form_types",
                                    options=self.formTypes,
                                    value=self._formTypes,
                                    className="dcc_control",
                                    style={'display': 'grid', 'font-size':'0.7em', 'grid-template-columns': 'auto auto auto auto'}
                                ),
                                # dcc.Dropdown(
                                #     id="form_types",
                                #     options=self.formTypes,
                                #     multi=True,
                                #     value=self._formTypes,
                                #     className="dcc_control",
                                # ),
                                html.P("Select SEC Industry Division(s):",
                                       className="control_label"),
                                dcc.RadioItems(
                                    id="sic_divisions_select_all",
                                    options=[{'label': 'Select All', 'value':1}, {'label': 'Select None', 'value':0}],
                                    value=0,
                                    className="dcc_control",
                                    style={'display': 'grid', 'font-size':'0.7em', 'grid-template-columns': 'auto auto auto auto'}
                                ),
                                dcc.Checklist(
                                    id="sic_divisions",
                                    options=self.sicDiv,
                                    value=self._sicDiv,
                                    className="dcc_control",
                                    style={'font-size':'0.7em'}
                                ),
                                # dcc.Dropdown(
                                #     id="sic_divisions",
                                #     options=self.sicDiv,
                                #     multi=True,
                                #     value=self._sicDiv,
                                #     className="dcc_control",
                                # ),
                            ],
                            className="pretty_container four columns",
                            id="cross-filter-options",
                        ),
                        html.Div(
                            [
                                html.H5("Total DB Info {}".format(
                                    self.dbInfo), style={"margin-top": "0px", "font-size":'1.2em'}),
                                html.Div(
                                    [
                                        html.Div(
                                            [html.H6(id="feeds_text", children=self.human_format(self.dbStats['CountFeeds'])), html.P("Feeds")],
                                            id="feeds",
                                            className="notlast mini_container",

                                        ),
                                        html.Div(
                                            [html.H6(id="filingsText", children=self.human_format(
                                                self.dbStats['CountFilings'])), html.P("Filings")],
                                            id="filings",
                                            className="notlast mini_container",
                                        ),
                                        html.Div(
                                            [html.H6(id="filesText", children=self.human_format(
                                                self.dbStats['CountFiles'])), html.P("Files")],
                                            id="files",
                                            className="notlast mini_container",
                                        ),
                                        html.Div(
                                            [html.H6(id="filersText", children=self.human_format(
                                                self.dbStats['CountFilers'])), html.P("Filers")],
                                            id="filers",
                                            className="notlast mini_container",
                                        ),
                                        html.Div(
                                            [html.H6(id="lastUpdateText", children=str(
                                                self.dbStats['LastUpdate'])), html.P("Last Updated")],
                                            id="lastUpdate",
                                            className="mini_container",
                                        ),
                                    ],
                                    id="info-container",
                                    className="row container-display",
                                ),
                                html.H5("Total Selected", style={
                                        "margin-top": "0px", "font-size":'1.2em'}),
                                html.Div(
                                    [
                                        html.Div(
                                            [html.H6(id="feedsText_slctd"),
                                             html.P("feeds")],
                                            id="feeds_slctd",
                                            className="notlast mini_container",
                                        ),
                                        html.Div(
                                            [html.H6(id="filingsText_slctd"), html.P(
                                                "Filings")],
                                            id="filings_slctd",
                                            className="notlast mini_container",
                                        ),
                                        html.Div(
                                            [html.H6(id="filerText_slctd"),
                                             html.P("Filers")],
                                            id="filer_slctd",
                                            className="notlast mini_container",
                                        ),
                                        html.Div(
                                            [html.H6(id="formsText_slctd"),
                                             html.P("Forms")],
                                            id="forms_slctd",
                                            className="notlast mini_container",
                                        ),
                                        html.Div(
                                            [html.H6(id="industryText_slctd"), html.P(
                                                ["Sub-Division Classifications"])],
                                            id="industry_slctd",
                                            className="mini_container",
                                        )
                                    ],
                                    id="info-container_slctd",
                                    className="row container-display",
                                ),
                                html.Div(
                                    [dcc.Graph(id="count_graph")],
                                    id="countGraphContainer",
                                    className="pretty_container",
                                ),
                            ],
                            id="right-column",
                            className="eight columns",
                        ),
                    ],
                    className="row flex-display",
                ),
                html.Div(
                    [
                        html.Div(
                            [dcc.Graph(id="forms_graph")],
                            className="pretty_container six columns",
                        ),
                        html.Div(
                            [dcc.Graph(id="industry_graph")],
                            className="pretty_container six columns",
                        ),
                    ],
                    className="row flex-display",
                ),
                html.Div(
                    [
                        html.Div(
                            [dcc.Graph(id="map_graph")],
                            className="pretty_container twelve columns",
                        ),
                    ],
                    className="row flex-display",
                ),
            ],
            id="mainContainer",
            style={"display": "flex", "flex-direction": "column"},
        )
        return layout

    def filter_dataframe(self, df, formType, division_name, year_slider):
        if not formType:
            formType = self._formTypes
        if not division_name:
            division_name = self._sicDiv

        dff = df[
            (df["division_name"].isin(division_name))
            &( df["formType"].isin(formType))
            & (df["year"] >= year_slider[0])
            & (df["year"] <= year_slider[1])
        ]
        return dff

    def update_boxes(self, form_types, sic_divisions, year_slider):
        df = self.filingsSummary
        data = self.filter_dataframe(
            df, form_types, sic_divisions, year_slider)
        filings = data['count'].sum()
        forms = data.formType.unique().__len__()
        industry = data.assignedSic.unique().__len__()
        filers = data.cikNumber.unique().__len__()
        feeds = data.feedId.unique().__len__()
        return self.human_format(feeds), self.human_format(filings), self.human_format(forms), self.human_format(industry), self.human_format(filers)

    def make_figures(self, form_types, sic_divisions, year_slider, time_freq_selector):
        df = self.filingsSummary
        filers_df = self.filersLocations
        _data = df.copy()
        _data['inlineXBRL'] = ['Inline XBRL' if x else 'XBRL' for x in _data['inlineXBRL']]
        data = self.filter_dataframe(_data,form_types, sic_divisions, year_slider)
        _helper = {
            'month': {'group': ['month', 'feedMonth', 'inlineXBRL'], 'sort':'feedMonth'},
            'quarter': {'group': ['quarter', 'endOfQuarter', 'inlineXBRL'], 'sort':'endOfQuarter'},
            'year': {'group': ['year','inlineXBRL'], 'sort':'year'}
        }
        grp = data.groupby(_helper[time_freq_selector]['group'])['count'].sum().reset_index()
        grp_sort = list(grp.sort_values(by=_helper[time_freq_selector]['sort'])[time_freq_selector].unique())
        grp_totals = grp.groupby(_helper[time_freq_selector]['group'][:-1])['count'].sum().reset_index()
        grp_totals.rename(columns={'count':'total'}, inplace=True)
        grp_final = pd.merge(grp, grp_totals, on=time_freq_selector)
        grp_final_sorted = grp_final.sort_values('inlineXBRL', ascending=False)
        fig = px.bar(data_frame=grp_final_sorted, x=time_freq_selector, y='count', color='inlineXBRL', custom_data=['total',])
        fig.update_traces(hovertemplate="%{x}: %{y:,.0f} forms (All Forms %{customdata[0]:,.0f})")
        fig.update_layout(title={'text': "Filings By {}".format(time_freq_selector), 'yanchor': 'top', 'x':0.5}, 
                            legend=dict(orientation="h", title='', yanchor="bottom", y=-0.25, xanchor="left", x=0),
                            xaxis={'type':'category','categoryorder':'array', 'categoryarray':grp_sort}, uniformtext_minsize=5)
        grp2_sort = list(data.groupby(['formType'])['count'].sum().reset_index().sort_values(by='count',ascending=True).formType)
        grp2_range = [len(set(grp2_sort))-7,len(set(grp2_sort))-.5] if len(set(grp2_sort)) > 8 else [0, len(set(grp2_sort))]
        grp2 = data.groupby(['formType', 'inlineXBRL'])['count'].sum().reset_index()
        grp2_totals = grp2.groupby('formType')['count'].sum().reset_index()
        grp2_totals.rename(columns={'count':'total'}, inplace=True)
        grp2_final = pd.merge(grp2, grp2_totals, on='formType')
        grp2_final_sorted = grp2_final.sort_values('inlineXBRL', ascending=False)
        fig2 = px.bar(data_frame=grp2_final_sorted, x='count', y='formType', color='inlineXBRL', orientation='h', custom_data=['total',])
        fig2.update_traces(hovertemplate="%{y}: %{x:,.0f} forms (All Forms %{customdata[0]:,.0f})")
        fig2.update_layout(title={'text': "Filings By Form", 'yanchor': 'top', 'x':0.5},
                            legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="left", x=0, title=''),
                                    yaxis={'categoryorder':'array', 'categoryarray':grp2_sort, 'range':grp2_range, 'title':''})
        
        grp3_sort = list(data.groupby(['division_name'])['count'].sum().reset_index().sort_values(by='count',ascending=True).division_name)
        grp3_range = [len(set(grp3_sort))-7,len(set(grp3_sort))-.5] if len(set(grp3_sort)) > 8 else [0, len(set(grp3_sort))]
        grp3 = data.groupby(['division_name', 'inlineXBRL'])['count'].sum().reset_index()
        grp3_totals = grp3.groupby('division_name')['count'].sum().reset_index()
        grp3_totals.rename(columns={'count':'total'}, inplace=True)
        grp3_final = pd.merge(grp3, grp3_totals, on='division_name')
        grp3_final_sorted = grp3_final.sort_values('inlineXBRL', ascending=False)
        fig3 = px.bar(data_frame=grp3_final_sorted, x='count', y='division_name', color='inlineXBRL', orientation='h', custom_data=['total',])
        fig3.update_traces(hovertemplate="%{y}: %{x:,.0f} forms (All Forms %{customdata[0]:,.0f})")
        fig3.update_layout(title={'text': "Filings By Industry",'yanchor': 'top', 'x':0.5},
                            legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="left", x=0, title=''),
                                    yaxis={'categoryorder':'array', 'categoryarray':grp3_sort, 'range':grp3_range, 'title':''}, )

        _data_filers = filers_df.copy()
        slcd_filers = list(data['cikNumber'].unique())
        data_filers = _data_filers[(_data_filers['cikNumber'].isin(slcd_filers))]
        grp_4 = data.groupby(['cikNumber'])['count'].sum().reset_index()
        filers_map = pd.merge(grp_4, data_filers, on='cikNumber')
        reGrp = filers_map.groupby(['code']).agg({'count':'sum', 'cikNumber':'count', 'longitude':'max', 'latitude':'max',
                                        'country': lambda x: x.iloc[0], 'stateProvince': lambda x: x.iloc[0]}).reset_index()
        reGrp.stateProvince = [x if x else '' for x in reGrp.stateProvince]
        reGrp.columns = ['code', 'Count of Filings','Count of Filers', 'longitude', 'latitude', 'Country', 'State']
        px.set_mapbox_access_token(self.mapbox_access_token)
        fig4 = px.scatter_mapbox(reGrp, lat="latitude", lon="longitude", size='Count of Filers', color='Count of Filings', hover_data=["State", "Country", "Count of Filers","Count of Filings"], 
                            size_max=15, zoom=3, mapbox_style="carto-positron", title='Filers By Location', 
                            center=dict(lat=38,lon=-94),)
        fig4.update_traces(hovertemplate="%{customdata[0]} %{customdata[1]}: %{marker.color:,.0f} forms - %{customdata[2]:,.0f} filers")
        # Count of Filers=%{customdata[2]}<br>latitude=%{lat}<br>longitude=%{lon}<br>State=%{customdata[0]}<br>Country=%{customdata[1]}<br>Count of Filings=%{marker.color}<extra></extra>
        fig4.update_layout(title={'yanchor': 'top', 'x':0.5})
        
        # for data in fig4.data:
        #     template = data.hovertemplate
        #     print(template)

        return fig, fig2, fig3, fig4

    def startDash(self, host, port=None, debug=False, asDaemon=True, threaded=True):
        landingPage = None
        p = None
        gettext.install('arelle')
        if not port:
            # find available port
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("", 0))
            s.listen(1)
            port = str(s.getsockname()[1])
            s.close()

        xhost = host
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('10.255.255.255', 1))
            xhost = s.getsockname()[0]
        finally:
            s.close()
        
        landingPage = "http://{}:{}".format(xhost, port)
        if threaded:
            p = threading.Thread(target=self.init, args=(host, port, debug), daemon=asDaemon)
            p.start()
            self.conn.cntlr.addToLog(_('Dash available at: {}'.format(landingPage)), messageCode="RssDB.Info", file="",  level=logging.INFO)
        
        else:
            self.init(host, port, debug)

        return (p, landingPage)

    def init(self, host, port=None, debug=False):
        self.app.run_server(host=host, port=port, debug=debug)
