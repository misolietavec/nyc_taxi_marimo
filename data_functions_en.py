import polars as pl
import plotly.express as px
import os, pickle
import numpy as np


def get_totals(frm, pick=True):
    columns = ['pick_day','pick_hour'] if pick else ['drop_day','drop_hour']
    df_total = frm.group_by(columns)\
                            .agg([pl.col('fare').sum().alias('total_fare'),\
                                  pl.col('passengers').sum().alias('total_pass'),
                                  pl.col('fare').count().alias('total_rides')])\
                            .sort(by=columns)
    return df_total


def total_graphs(frm, pick=True, height=400, width=750, what=['total_pass', 'total_rides', 'total_fare'],
                 monthly=False, day=True):
    if not monthly:
        frm = get_totals(frm) if pick else get_totals(frm, pick=False)
        column = 'pick_hour' if pick else 'drop_hour'
    else:
        column = ('pick_day' if pick else 'drop_day') if day else ('pick_hour' if pick else 'drop_hour')
    ylabel = 'Pickups' if pick else 'Dropoffs' 
    max_jaz, max_pas, max_far = frm['total_rides'].max(), frm['total_pass'].max(), frm['total_fare'].max()
    coeff = max(max_jaz, max_pas) / max_far
    trzba = frm['total_fare'].sum()
    frm = frm.with_columns(pl.col('total_fare') * coeff)
    scalestr = f"Fare multiplied by {coeff:.3f}, total fare {trzba:.2f}" if ('total_fare' in what) else ''
    colname = 'Hour' if not (monthly and day) else 'Day'
    graf = px.bar(frm, x=column, y=what, barmode='group', height=height, width=width, title=scalestr, 
                  labels={column: colname, 'value': ylabel})
    if monthly and day:
        xtext = 4 * ['Thu', 'Fri', 'Sat', 'Sun', 'Mon', 'Tue', 'Wed'] + ['Thu','Fri','Sat']
        xtext = [f"{ind + 1}, {dni}" for ind, dni in enumerate(xtext)]
        graf.update_layout(xaxis=dict(tickmode='array', tickvals=list(range(1, 32)),
                           ticktext = xtext, tickangle=70), yaxis=dict(title=ylabel))
    else:
        graf.update_layout(xaxis=dict(tickmode='array', tickvals=list(range(24))), yaxis=dict(title=ylabel))
    return graf


# We will make plots only once, then load them from pickle file
def make_graphs(df, create=True):
    plotfile = 'data/dfdays_en.pic'
    if create:
        dfdays = {}
        for day in range(1, 32):
            pick_df = df.filter(pl.col('pick_day') == day) 
            drop_df = df.filter(pl.col('drop_day') == day)
            dfdays[day] = {}
            dfdays[day]['pick_graph'] = total_graphs(pick_df) 
            dfdays[day]['drop_graph'] = total_graphs(drop_df, pick=False)
            pickle.dump(dfdays, open(plotfile,'wb'))
    else:
        dfdays = pickle.load(open(plotfile,'rb'))
    return dfdays


def monthly_frame(frm, pick=True, day=True):
    column = ('pick_day' if pick else 'drop_day') if day else ('pick_hour' if pick else 'drop_hour')
    df_month = frm.group_by(column).agg([pl.col('fare').sum().alias('total_fare'), 
                                         pl.col('passengers').sum().alias('total_pass'),
                                         pl.col('fare').count().alias('total_rides')]).sort(by=column)
    return df_month


def weekday_plot(frm):
    df_wd = frm.with_columns(pl.date(2015, 1, pl.col('pick_day')).dt.weekday().alias('wday'))\
                           .select(['pick_day', 'wday'])
    wdcount = np.array([4, 4, 4, 5, 5, 5, 4])
    wstat = df_wd.group_by(pl.col('wday')).agg(pl.col('wday').count().alias('counts')).sort(by='wday')
    wstat = wstat.with_columns(pl.col('counts') / wdcount)  # mean for one weekday
    graf = px.bar(wstat, x='wday', y='counts', barmode='group', orientation='v', width=750, height=400)
    xtext = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    graf.update_layout(xaxis=dict(tickmode='array', tickvals=list(range(1, 8)), title='Weekday',
                       ticktext=xtext, tickangle=0), yaxis=dict(title="Rides"))
    return graf

# Plots - pickups and dropoffs by hour (rides, passenger count, fare), total
def static_graphs(frm):
    pick_days = total_graphs(monthly_frame(frm), height=350, width=900, monthly=True)
    drop_days = total_graphs(monthly_frame(frm, pick=False), height=350, width=900, monthly=True, pick=False)
    pick_hours = total_graphs(monthly_frame(frm, day=False), height=350, width=900, monthly=True, day=False)
    drop_hours = total_graphs(monthly_frame(frm, pick=False, day=False), height=350, width=900, 
                              monthly=True, pick=False, day=False) 
    return pick_days, drop_days, pick_hours, drop_hours
