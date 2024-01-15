#%% Local Code Only

import socket
host_name = socket.gethostname()
local_name = 'eric-Latitude-3440'
if host_name == local_name:
    from dotenv import load_dotenv
    load_dotenv('./.env', override=True)


#%% Imports

from dbharbor.bigquery import SQL
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
cat_list = os.getenv('CAT_LIST').split(',')


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
        filepath_html = 'file:\\\\' + filepath_html
    else:
        filepath_html = 'file://' + filepath_html
    with ChromeDriver(no_sandbox=True, window_size='1920,1080', use_chromium=True) as driver:
        driver.get(filepath_html)
        chart = driver.find_element(by='xpath', value='/html/body/table')
        chart.screenshot(filepath_png)

fmt = lambda x: '-' if pd.isna(x) or x == 0 else '${:,.0f}'.format(x) if x >= 0 else '$({:,.0f})'.format(abs(x))


#%% SQL Connector

logger.info('SQL Connector')
con = SQL(os.getenv('BIGQUERY_CRED'))


#%% Get Actuals Data

logger.info('Get Actuals Data')

df = con.read("""
SELECT effective_date
    , new_category
    , amount
FROM `bbg-platform.analytics.v_dashboard_revenue`;
""")

df['effective_date'] = pd.to_datetime(df['effective_date'])
df['yrmnth'] = df['effective_date'].dt.to_period('M')


#%% Get Budget Data

logger.info('Get Budget Data')

# df_budget = con.read("""
# select eom
#   , category_budget
#   , sum(amount) as amount
# from `bbg-platform.analytics_stage.fct_budget_2023`
# group by 1, 2
# order by 1, 2;
# """)

# df_budget['eom'] = pd.to_datetime(df_budget['eom'])
# df_budget['yrmnth'] = df_budget['eom'].dt.to_period('M')

df_budget = df[df['effective_date'].dt.year == dt.datetime.now().year - 1]
df_budget = df_budget.groupby(['yrmnth', 'new_category'])['amount'].sum()
df_budget = pd.DataFrame(df_budget).reset_index()
df_budget.rename({'yrmnth':'eom', 'new_category':'category_budget'}, axis=1, inplace=True)
df_budget['eom'] = df_budget['eom'].apply(lambda x: x.to_timestamp() + pd.offsets.MonthEnd(0))


#%% Daily Data ################################################################################################################

logger.info('Daily Data')

eom = dt.date.today() + dt.timedelta(days=-1) + pd.offsets.MonthEnd(0)
recent_date = eom + pd.offsets.MonthBegin(-1)
recent_date = dt.datetime(recent_date.year, recent_date.month, recent_date.day)
peom = eom + pd.offsets.MonthEnd(-12)

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


# Month's budget Chart
dfs_budget = pd.DataFrame()
dfs_budget.index = dfs.index
dfs_budget['budget'] = df_budget[df_budget['eom'] == peom]['amount'].sum()

# Month's budget Table
dfg_aggr_budget = df_budget[df_budget['eom'] == peom][['category_budget', 'amount']]
dfg_aggr_budget.set_index('category_budget', inplace=True)
dfg_aggr_budget = pd.concat([dfg_aggr, dfg_aggr_budget.T], axis=0)
dfg_aggr_budget = dfg_aggr_budget.fillna(0)
dfg_aggr_budget['Total'] = dfg_aggr_budget.sum(axis=1)
dfg_aggr_diff = dfg_aggr_budget.iloc[0] - dfg_aggr_budget.iloc[1]
dfg_aggr_budget = pd.concat([dfg_aggr_budget, pd.DataFrame(dfg_aggr_diff).T])
dfg_aggr_budget.index = ['Current Year', 'Prior Year', 'Variance']
for x in cat_list:
    if x not in dfg_aggr_budget.columns:
        dfg_aggr_budget[x] = 0
dfg_aggr_budget = dfg_aggr_budget[cat_list + ['Total']]
dfg_aggr_budget.reset_index(inplace=True, names='Type')


#%% Daily Chart ################################################################################################################

logger.info('Daily Chart')

filepath_day_chart = './day_chart.png'

x = date2num(dfs.index)

# Daily Bars
fig, ax = plt.subplots(figsize=(10, 4.8))
ax.bar(x, height=dfs.sum(axis=1), label='Daily Total')
for i, y in enumerate(dfs.sum(axis=1)):
    ax.annotate('{:,.0f}'.format(y*1e-3), (x[i], y), ha='center', va='bottom')

# Cumulative Line
ax.plot(x, dfs.sum(axis=1).cumsum(), color='black', label='MTD Total')
i, y = list(enumerate(dfs.sum(axis=1).cumsum()))[-1]
ax.annotate('{:,.1f}M'.format(y*1e-6), (x[i], y), ha='left', va='center')

ax.plot(x, dfs_budget, color='red', ls='--', label='Prior Year')
i, y = list(enumerate(dfs_budget['budget']))[0]
ax.annotate('{:,.1f}M'.format(y*1e-6), (x[i], y), ha='right', va='center', color='red')

# Formatting
ax.set_title('Current Month Cash', loc='center')
ax.set_xlabel('Day')
ax.set_ylabel("Dollars in thousands")
ax.legend(loc='best')

ax.yaxis.set_major_formatter(lambda x, pos: '{:,.0f}'.format(x*1e-3))
ax.xaxis_date()
fig.autofmt_xdate()
plt.tight_layout()

plt.savefig(filepath_day_chart)


#%% Daily Table ################################################################################################################

logger.info('Daily Table')

filepath_day_table = './day_table.png'
filepath_day_table_html = './day_table.html'

clmn_format_dict = {}
for clmn in cat_list + ['Total']:
    clmn_format_dict[clmn] = fmt

dfg_all_formatted = dfg_all.style\
    .set_caption("Current Month Cash by Product by Day")\
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
            'selector': 'tbody tr:last-child td',
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


#%% MTD Budget Table ################################################################################################################

logger.info('MTD Budget Table')

filepath_budget_table = './mtd_budget_table.png'
filepath_budget_table_html = './mtd_budget_table.html'

clmn_format_dict = {}
for clmn in cat_list + ['Total']:
    clmn_format_dict[clmn] = fmt

dfg_aggr_budget_formatted = dfg_aggr_budget.style\
    .set_caption("Current Month Cash by Product vs Prior Year")\
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
            'selector': 'tbody tr:last-child td',
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

with open(filepath_budget_table_html, 'w') as f:
    html = dfg_aggr_budget_formatted.to_html()
    html = html.replace('<style type="text/css">', '<style type="text/css">\ntable {\n\tborder-spacing: 0;\n}')
    f.write(html)

screenshot(filepath_budget_table_html, filepath_budget_table)


#%% Monthly Data ################################################################################################################

logger.info('Monthly Data')

dfg = df[df['yrmnth'].dt.year == eom.year]
dfg = dfg.groupby(['new_category', 'yrmnth'])['amount'].sum()
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


# Cumulative Budget
dfg_budget = pd.DataFrame()
dfg_budget.index = dfg.index
dfg_budget = df_budget[df_budget['eom'] <= peom]
dfg_budget = dfg_budget[['eom', 'amount']]
dfg_budget.set_index('eom', inplace=True)
dfg_budget = dfg_budget.groupby('eom')['amount'].sum().cumsum()
dfg_budget = pd.DataFrame(dfg_budget)

# YTD budget Table
dfg_aggr_budget = df_budget[df_budget['eom'] <= peom].groupby('category_budget')['amount'].sum()
dfg_aggr_budget = pd.DataFrame(dfg_aggr_budget)
dfg_aggr_budget = pd.concat([dfg_aggr, dfg_aggr_budget.T], axis=0)
dfg_aggr_budget = dfg_aggr_budget.fillna(0)
dfg_aggr_budget['Total'] = dfg_aggr_budget.sum(axis=1)
dfg_aggr_diff = dfg_aggr_budget.iloc[0] - dfg_aggr_budget.iloc[1]
dfg_aggr_budget = pd.concat([dfg_aggr_budget, pd.DataFrame(dfg_aggr_diff).T])
dfg_aggr_budget.index = ['Current Year', 'Prior Year', 'Variance']
dfg_aggr_budget = dfg_aggr_budget[cat_list + ['Total']]
dfg_aggr_budget.reset_index(inplace=True, names='Type')


#%% Monthly Chart ################################################################################################################

logger.info('Monthly Chart')

filepath_month_chart = './month_chart.png'

x = dfg.index.astype(str)

fig, ax = plt.subplots()

# Monthly Bars
ax.bar(x, dfg.sum(axis=1), label='Monthly Total')
for i, y in enumerate(dfg.sum(axis=1)):
    ax.annotate('{:,.1f}'.format(y*1e-6), (x[i], y), ha='center', va='bottom')

# Cumulative Line
ax.plot(x, dfg.sum(axis=1).cumsum(), color='black', label='YTD Total')
i, y = list(enumerate(dfg.sum(axis=1).cumsum()))[-1]
ax.annotate('{:,.1f}'.format(y*1e-6), (x[i], y), ha='left', va='center')

# Budget Line
ax.plot(x, dfg_budget['amount'], color='red', ls='--', label='YTD Budget')
i, y = list(enumerate(dfg_budget['amount']))[-1]
ax.annotate('{:,.1f}'.format(y*1e-6), (x[i], y), ha='left', va='center', color='red')

ax.set_title('Cash by Month', loc='center')
ax.set_ylabel('Dollars in millions')
ax.set_xlabel('Month')
ax.legend(loc='best')

ax.yaxis.set_major_formatter(lambda x, pos: '{:,.0f}'.format(x*1e-6))
plt.xticks(rotation=45)
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
    .set_caption("Cash by Product by Month")\
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
            'selector': 'tbody tr:last-child td',
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


#%% YTD Budget Table ################################################################################################################

logger.info('YTD Budget Table')

filepath_ytd_budget = './ytd_budget.png'
filepath_ytd_budget_html = './ytd_budget.html'

fmt = lambda x: '-' if pd.isna(x) else '${:,.0f}'.format(x) if x >= 0 else '$({:,.0f})'.format(abs(x))

clmn_format_dict = {}
for clmn in cat_list + ['Total']:
    clmn_format_dict[clmn] = fmt


dfg_all_formatted = dfg_aggr_budget.style\
    .set_caption("YTD Cash by Product vs Prior Year")\
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
            'selector': 'tbody tr:last-child td',
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
    
# dfi.export(dfg_all_formatted, filepath_ytd_budget)

with open(filepath_ytd_budget_html, 'w') as f:
    html = dfg_all_formatted.to_html()
    html = html.replace('<style type="text/css">', '<style type="text/css">\ntable {\n\tborder-spacing: 0;\n}')
    f.write(html)

screenshot(filepath_ytd_budget_html, filepath_ytd_budget)


#%% Send Email Update ################################################################################################################

logger.info('Send Email Update')

body = ["Good morning!  Here is today's update:<br><br>",
        # filepath_month_chart,
        # "<br>",
        # filepath_ytd_budget,
        # "<br>",
        # filepath_month_table,
        # "<br>",
        filepath_day_chart,
        "<br>",
        filepath_budget_table,
        "<br>",
        filepath_day_table,
        # "<br>For additional views please visit the following link:",
        # "https://lookerstudio.google.com/reporting/b656cb16-6007-467b-85ef-b412931d5b7a",
        "<br>Have a great day!"]

SendEmail(to_email_addresses=os.getenv('EMAIL_FAIL')
        , subject= 'MM Daily Dash - ' + dt.date.today().strftime('%m-%d-%Y')
        , body=body
        , user=os.getenv('EMAIL_UID')
        , password=os.getenv('EMAIL_PWD')
        , bcc_email_addresses=os.getenv('EMAIL_SEND')
        )


# %%
