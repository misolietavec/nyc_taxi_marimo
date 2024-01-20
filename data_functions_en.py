import polars as pl
import plotly.express as px
import os, pickle


def get_totals(frm, pick=True):
    columns = ['pick_day','pick_hour'] if pick else ['drop_day','drop_hour']
    df_total = frm.group_by(columns)\
                            .agg([pl.col('fare').sum().alias('total_fare'),\
                                  pl.col('passengers').sum().alias('total_pass'),
                                  pl.col('fare').count().alias('total_rides')])\
                            .sort(by=columns)
    return df_total

def get_monthly(frm, pick=True):
    column = 'pick_day' if pick else 'drop_day'
    

def total_graphs(frm, pick=True, height=400, what=['total_pass', 'total_rides', 'total_fare'], monthly=False):
    if not monthly:
        frm = get_totals(frm) if pick else get_totals(frm, pick=False)
        column = 'pick_hour' if pick else 'drop_hour'
    else: 
        column = 'pick_day' if pick else 'drop_day'
    ylabel = 'Pickups' if pick else 'Dropoffs' 
    max_jaz, max_pas, max_far = frm['total_rides'].max(), frm['total_pass'].max(), frm['total_fare'].max()
    coeff = max(max_jaz, max_pas) / max_far
    trzba = frm['total_fare'].sum()
    frm = frm.with_columns(pl.col('total_fare') * coeff)
    scalestr = f"Fare multiplied by {coeff:.3f}, total fare {trzba:.2f}" \
               if ('total_fare' in what) and not pick else ''
    colname = 'Hour' if not monthly else 'Day'
    graf = px.bar(frm, x=column, y=what, barmode='group', height=height, width=750, title=scalestr, 
                  labels={column: colname, 'value': ylabel})
    return graf


# We will make plots only once, then load them from pickle file
def make_graphs(df, create=True):
    plotfile = 'data/dfdays.pic'
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

def monthly_frame(frm, pick=True):
    column = 'pick_day' if pick else 'drop_day'
    df_month = frm.group_by(column).agg([pl.col('fare').sum().alias('total_fare'), 
                                         pl.col('passengers').sum().alias('total_pass'),
                                         pl.col('fare').count().alias('total_rides')]).sort(by=column)
    return df_month

# Plots - pickups and dropoffs by hour (rides, passenger count, fare), total
def static_graphs(frm):
    pick_bar = total_graphs(monthly_frame(frm), height=350, monthly=True)
    drop_bar = total_graphs(monthly_frame(frm, pick=False), height=350, monthly=True, pick=False)
    return pick_bar, drop_bar 
