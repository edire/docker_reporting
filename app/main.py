#%% Local Code Only

import socket
host_name = socket.gethostname()
if host_name == 'powerhouse':
    from dotenv import load_dotenv
    load_dotenv('./.env')
    load_dotenv('../.env')


#%% Imports

from ddb.mysql import SQL
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import dataframe_image as dfi
from demail.gmail import SendEmail
import os
import dlogging


logger = dlogging.NewLogger(__file__, use_cd=True)

cat_list = ['Coaching', 'Project Next', 'Project Next Level', 'Legacy', 'Mastermind', 'Other']


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


#%% Pivot Data

logger.info('Pivot Data')

dfg = df.groupby(['new_category', 'yrmnth'])['amount'].sum()
dfg = dfg.reset_index(drop=False)
dfg = dfg.sort_values('new_category')
dfg = dfg.pivot(index='yrmnth', columns='new_category', values='amount')
dfg = dfg[cat_list]


#%% Chart Cash by Product by Month

logger.info('Chart Cash by Product by Month')

filepath_rev_prod_mnth = './rev_prod_mnth.png'

ax = dfg.plot.bar(stacked=True)
for x, y in enumerate(dfg.sum(axis=1)):
    ax.annotate('${:,.1f}M'.format(y*1e-6), (x, y), ha='center', va='bottom')
ax.set_title('Cash by Product by Month', loc='center')
ax.set_ylabel('Cash')
ax.set_xlabel('Month')
ax.legend()
plt.gcf().autofmt_xdate()
ax.yaxis.set_major_formatter(lambda x, pos: '${:,.0f}M'.format(x*1e-6))
plt.tight_layout()
plt.savefig(filepath_rev_prod_mnth)


#%% Recent Week Table

logger.info('Recent Week Table')

recent_date = dt.date.today() + dt.timedelta(-1) + pd.offsets.MonthEnd(0) + pd.offsets.MonthBegin(-1)
recent_date = dt.datetime(recent_date.year, recent_date.month, recent_date.day)

dfs = df[df['effective_date'] >= recent_date]
dfs = dfs.pivot(index='effective_date', columns='new_category', values='amount')
dfs.index = dfs.index.date

dfs_aggr = dfs.sum(axis=0, numeric_only=True)
dfs_aggr = pd.DataFrame(dfs_aggr).T
dfs_aggr.index = ['Total']

dfs_all = pd.concat([dfs, dfs_aggr])
dfs_all.columns.name = None

dfs_aggc = dfs_all.sum(axis=1, numeric_only=True)
dfs_aggc = pd.DataFrame(dfs_aggc, index=dfs_all.index, columns=['Total'])

dfs_all = pd.concat([dfs_all, dfs_aggc], axis=1)
dfs_all = dfs_all.reset_index(drop=False, names='Date')
dfs_all = dfs_all[['Date'] + cat_list + ['Total']]


#%% Format Picture of Table for Email

logger.info('Format Picture of Table for Email')

filepath_rev_prod_daily = './rev_prod_daily.png'

fmt = lambda x: '-' if pd.isna(x) else '${:,.0f}'.format(x) if x >= 0 else '$({:,.0f})'.format(abs(x))

clmn_format_dict = {}
for clmn in cat_list + ['Total']:
    clmn_format_dict[clmn] = fmt


dfs_all_formatted = dfs_all.style\
    .set_caption("Current Month Cash by Product by Day")\
    .hide(axis="index")\
    .set_properties(**{'text-align': 'left'})\
    .set_properties(**{'font-size': '11px;'})\
    .set_properties(**{'font-family': 'Century Gothic, sans-serif;'})\
    .set_properties(**{'padding': '3px 20px 3px 5px;'})\
    .set_properties(**{'width': 'auto'})\
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
                font-size: 12px;\
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
    

dfi.export(dfs_all_formatted, filepath_rev_prod_daily)


#%% Send Email Update

logger.info('Send Email Update')

body = ["Good morning!<br><br>Here is today's update:<br><br>",
        filepath_rev_prod_mnth,
        '<br><br>',
        filepath_rev_prod_daily,
        '<br><br>Have a great day!']

SendEmail(to_email_addresses=os.getenv('email_send')
        , subject= 'Daily Update - ' + dt.date.today().strftime('%m-%d-%Y')
        , body=body
        , user=os.getenv('email_uid')
        , password=os.getenv('email_pwd'))


# %%
