#%% Local Code Only

import socket
host_name = socket.gethostname()
local_name = 'powerhouse'
if host_name == local_name:
    from dotenv import load_dotenv
    load_dotenv('./.env')


#%% Imports

from ddb.mysql import SQL
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
# import dataframe_image as dfi
from demail.gmail import SendEmail
import os
import dlogging
from matplotlib.dates import date2num
from dwebdriver import ChromeDriver


logger = dlogging.NewLogger(__file__, use_cd=True)
cat_list = os.getenv('cat_list').split(',')


#%% Functions

logger.info('Create Functions')

def df_add_missing_clmns(df, clist = cat_list):
    for x in clist:
        if x not in df.columns:
            df[x] = 0
    return df

def screenshot(filepath_html, filepath_png):
    filepath_html = os.path.realpath(filepath_html)
    if host_name == local_name:
        filepath_html = 'file:\\' + filepath_html
    else:
        filepath_html = 'file://' + filepath_html
    with ChromeDriver(no_sandbox=True, window_size='1920,1080') as driver:
        driver.get(filepath_html)
        chart = driver.find_element(by='xpath', value='/html/body/table')
        chart.screenshot(filepath_png)


#%% SQL Connector

logger.info('SQL Connector')

con = SQL(server=os.getenv('mysql_server'),
        db=os.getenv('mysql_db'),
        uid=os.getenv('mysql_uid'),
        pwd=os.getenv('mysql_pwd')
        )


#%% Get Data

logger.info('Get Data')

df = con.read("""SELECT effective_date
, new_category
, amount
FROM vDashboard_Revenue;""")

df['effective_date'] = pd.to_datetime(df['effective_date'])
df['yrmnth'] = df['effective_date'].dt.to_period('M')


#%% Daily Data ################################################################################################################

logger.info('Daily Data')

recent_date = dt.date.today() + dt.timedelta(-1) + pd.offsets.MonthEnd(0) + pd.offsets.MonthBegin(-1)
recent_date = dt.datetime(recent_date.year, recent_date.month, recent_date.day)

dfs = df[df['effective_date'] >= recent_date]
dfs = dfs.pivot(index='effective_date', columns='new_category', values='amount')
dfs.index = dfs.index.date

dfg_aggr = dfs.sum(axis=0, numeric_only=True)
dfg_aggr = pd.DataFrame(dfg_aggr).T
dfg_aggr.index = ['Total']

dfg_all = pd.concat([dfs, dfg_aggr])
dfg_all.columns.name = None

dfg_aggc = dfg_all.sum(axis=1, numeric_only=True)
dfg_aggc = pd.DataFrame(dfg_aggc, index=dfg_all.index, columns=['Total'])

dfg_all = pd.concat([dfg_all, dfg_aggc], axis=1)
dfg_all = dfg_all.reset_index(drop=False, names='Date')
dfg_all = df_add_missing_clmns(dfg_all)
dfg_all = dfg_all[['Date'] + cat_list + ['Total']]


#%% Daily Chart ################################################################################################################

logger.info('Daily Chart')

filepath_day_chart = './day_chart.png'

width = 0.3
x = date2num(dfs.index)

fig, ax = plt.subplots(figsize=(10, 4.8))
ax.bar(x, height=dfs.sum(axis=1))
for i, y in enumerate(dfs.sum(axis=1)):
    ax.annotate('${:,.0f}K'.format(y*1e-3), (x[i], y), ha='center', va='bottom')

ax2 = ax.twinx()
ax2.plot(x, dfs.sum(axis=1).cumsum(), color='black')

ax.set_title('Current Month Net Cash', loc='center')
ax.set_ylabel('Net Cash')
ax.set_xlabel('Day')
ax2.set_ylabel('Cumulative Net Cash')

ax.yaxis.set_major_formatter(lambda x, pos: '${:,.0f}K'.format(x*1e-3))
ax2.yaxis.set_major_formatter(lambda x, pos: '${:,.0f}K'.format(x*1e-3))
ax.xaxis_date()
fig.autofmt_xdate()
plt.tight_layout()

plt.savefig(filepath_day_chart)


#%% Daily Table ################################################################################################################

logger.info('Daily Table')

filepath_day_table = './day_table.png'
filepath_day_table_html = './day_table.html'

fmt = lambda x: '-' if pd.isna(x) else '${:,.0f}'.format(x) if x >= 0 else '$({:,.0f})'.format(abs(x))

clmn_format_dict = {}
for clmn in cat_list + ['Total']:
    clmn_format_dict[clmn] = fmt


dfg_all_formatted = dfg_all.style\
    .set_caption("Current Month Net Cash by Product by Day")\
    .hide(axis="index")\
    .set_properties(**{'text-align': 'left'})\
    .set_properties(**{'font-size': '14px;'})\
    .set_properties(**{'font-family': 'Century Gothic, sans-serif;'})\
    .set_properties(**{'padding': '3px 20px 3px 5px;'})\
    .set_table_styles([
        # Caption
        {
            'selector': 'caption',
            'props': 'font-weight: bold;\
                font-size: 18px;\
                font-family: Century Gothic, sans-serif;\
                padding: 0px 0px 5px 0px;'
        },
        # Column Headers
        {
            'selector': 'thead th',
            'props': 'background-color: #FFFFFF;\
                color: #305496;\
                border-bottom: 2px solid #305496;\
                text-align: left;\
                font-size: 14px;\
                font-family: Century Gothic, sans-serif;\
                padding: 0px 20px 0px 5px;'
        },
        # Last Column Header
        {
            'selector': 'thead th:last-child',
            'props': 'color: black;'
        },
        # Even Rows
        {
            'selector': 'tbody tr:nth-child(even)',
            'props': 'background-color: white;\
                color: black;'
        },
        # Odd Rows
        {
            'selector': 'tbody tr:nth-child(odd)',
            'props': 'background-color: #D9E1F2;'
        },
        # Last Row
        {
            'selector': 'tbody tr:last-child',
            'props': 'font-weight: bold;\
                border-top: 2px solid #305496;'
        },
        # First Column
        {
            'selector': 'tbody td:first-child',
            'props': 'border-right: 2px solid #305496;'
        },
        # Last Column
        {
            'selector': 'tbody td:last-child',
            'props': 'font-weight: bold;\
                border-left: 2px solid #305496;'
        },
        ])\
    .format(clmn_format_dict)

# dfi.export(dfg_all_formatted, filepath_day_table)

with open(filepath_day_table_html, 'w') as f:
    html = dfg_all_formatted.to_html()
    html = html.replace('<style type="text/css">', '<style type="text/css">\ntable {\n\tborder-spacing: 0;\n}')
    f.write(html)

screenshot(filepath_day_table_html, filepath_day_table)


#%% Monthly Data ################################################################################################################

logger.info('Monthly Data')

dfg = df.groupby(['new_category', 'yrmnth'])['amount'].sum()
dfg = dfg.reset_index(drop=False)
dfg = dfg.sort_values('new_category')
dfg = dfg.pivot(index='yrmnth', columns='new_category', values='amount')
dfg = df_add_missing_clmns(dfg)
dfg = dfg[cat_list]

dfg_aggr = dfg.sum(axis=0, numeric_only=True)
dfg_aggr = pd.DataFrame(dfg_aggr).T
dfg_aggr.index = ['Total']

dfg_all = pd.concat([dfg, dfg_aggr])
dfg_all.columns.name = None

dfg_aggc = dfg_all.sum(axis=1, numeric_only=True)
dfg_aggc = pd.DataFrame(dfg_aggc, index=dfg_all.index, columns=['Total'])

dfg_all = pd.concat([dfg_all, dfg_aggc], axis=1)
dfg_all = dfg_all.reset_index(drop=False, names='Date')
dfg_all = df_add_missing_clmns(dfg_all)
dfg_all = dfg_all[['Date'] + cat_list + ['Total']]


#%% Monthly Chart ################################################################################################################

logger.info('Monthly Chart')

filepath_month_chart = './month_chart.png'

fig, ax = plt.subplots()
ax = dfg.sum(axis=1).plot.bar()
for x, y in enumerate(dfg.sum(axis=1)):
    ax.annotate('${:,.1f}M'.format(y*1e-6), (x, y), ha='center', va='bottom')
ax.set_title('Net Cash by Month', loc='center')
ax.set_ylabel('Net Cash')
ax.set_xlabel('Month')
plt.gcf().autofmt_xdate()
ax.yaxis.set_major_formatter(lambda x, pos: '${:,.0f}M'.format(x*1e-6))
plt.tight_layout()
plt.savefig(filepath_month_chart)


#%% Monthly Table ################################################################################################################

logger.info('Monthly Table')

filepath_month_table = './month_table.png'
filepath_month_table_html = './month_table.html'

fmt = lambda x: '-' if pd.isna(x) else '${:,.0f}'.format(x) if x >= 0 else '$({:,.0f})'.format(abs(x))

clmn_format_dict = {}
for clmn in cat_list + ['Total']:
    clmn_format_dict[clmn] = fmt


dfg_all_formatted = dfg_all.style\
    .set_caption("Net Cash by Product by Month")\
    .hide(axis="index")\
    .set_properties(**{'text-align': 'left'})\
    .set_properties(**{'font-size': '14px;'})\
    .set_properties(**{'font-family': 'Century Gothic, sans-serif;'})\
    .set_properties(**{'padding': '3px 20px 3px 5px;'})\
    .set_table_styles([
        # Caption
        {
            'selector': 'caption',
            'props': 'font-weight: bold;\
                font-size: 18px;\
                font-family: Century Gothic, sans-serif;\
                padding: 0px 0px 5px 0px;'
        },
        # Column Headers
        {
            'selector': 'thead th',
            'props': 'background-color: #FFFFFF;\
                color: #305496;\
                border-bottom: 2px solid #305496;\
                text-align: left;\
                font-size: 14px;\
                font-family: Century Gothic, sans-serif;\
                padding: 0px 20px 0px 5px;'
        },
        # Last Column Header
        {
            'selector': 'thead th:last-child',
            'props': 'color: black;'
        },
        # Even Rows
        {
            'selector': 'tbody tr:nth-child(even)',
            'props': 'background-color: white;\
                color: black;'
        },
        # Odd Rows
        {
            'selector': 'tbody tr:nth-child(odd)',
            'props': 'background-color: #D9E1F2;'
        },
        # Last Row
        {
            'selector': 'tbody tr:last-child',
            'props': 'font-weight: bold;\
                border-top: 2px solid #305496;'
        },
        # First Column
        {
            'selector': 'tbody td:first-child',
            'props': 'border-right: 2px solid #305496;'
        },
        # Last Column
        {
            'selector': 'tbody td:last-child',
            'props': 'font-weight: bold;\
                border-left: 2px solid #305496;'
        },
        ])\
    .format(clmn_format_dict)
    
# dfi.export(dfg_all_formatted, filepath_month_table)

with open(filepath_month_table_html, 'w') as f:
    html = dfg_all_formatted.to_html()
    html = html.replace('<style type="text/css">', '<style type="text/css">\ntable {\n\tborder-spacing: 0;\n}')
    f.write(html)

screenshot(filepath_month_table_html, filepath_month_table)


#%% Send Email Update ################################################################################################################

logger.info('Send Email Update')

body = ["Good morning!  Here is today's update:<br><br>",
        filepath_day_chart,
        "<br>",
        filepath_day_table,
        "<br>",
        filepath_month_chart,
        "<br>",
        filepath_month_table,
        "<br>For additional views please visit the following link:",
        "https://lookerstudio.google.com/reporting/b656cb16-6007-467b-85ef-b412931d5b7a",
        "<br>Have a great day!"]

SendEmail(to_email_addresses=os.getenv('email_send')
        , subject= 'MM Daily Dash - ' + dt.date.today().strftime('%m-%d-%Y')
        , body=body
        , user=os.getenv('email_uid')
        , password=os.getenv('email_pwd'))


# %%
