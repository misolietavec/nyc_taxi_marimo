import polars as pl
import plotly.express as px
import os, pickle


def passengers_data(frm, pick=True, height=400):
    column = 'pick_dt' if pick else 'drop_dt'
    ylabel = 'Pickups' if pick else 'Dropoffs'
    by_hours = frm.group_by(frm[column].dt.hour()) # pick_hour or drop_hour
    pgs = by_hours.agg(pl.col('passengers').sum())
    pgs = pgs.with_columns(by_hours.agg(pl.col('fare').count().alias('rides').cast(pl.Float32)))
    pgs = pgs.with_columns(by_hours.agg(pl.col('fare').sum()))    
    max_jaz, max_pas, max_far = pgs['rides'].max(), pgs['passengers'].max(), pgs['fare'].max()
    coeff = max(max_jaz, max_pas) / max_far
    pgs = pgs.with_columns(pl.col('fare') * coeff)
    trzba = pgs['fare'].sum()
    scalestr = f"Fare multiplied by {coeff:.3f}, total fare {trzba:.2f}"
    
    graf = px.bar(pgs, x=column, y=['passengers', 'rides', 'fare'], 
                  barmode='group', height=height, width=750, # title=scalestr, 
                  labels={column: 'hours', 'value': ylabel})
    return graf


# We will make plots only once, then load them from pickle file
def make_graphs(df):
    plotfile = 'data/dfdays_en.pic'
    if not os.path.isfile(plotfile):
        dfdays = {}
        for day in range(1, 32):
            pick_df = df.filter(pl.col('pick_dt').dt.day() == day) 
            drop_df = df.filter(pl.col('drop_dt').dt.day() == day)
            dfdays[day] = {}
            dfdays[day]['pick_graph'] = passengers_data(pick_df) 
            dfdays[day]['drop_graph'] = passengers_data(drop_df, pick=False)
            pickle.dump(dfdays, open(plotfile,'wb'))
    else:
        dfdays = pickle.load(open(plotfile,'rb'))
    return dfdays


# Plots - pickups and dropoffs by hour (rides, passenger count, fare), total
def static_graphs(frm):
    pick_bar = passengers_data(frm, height=300)
    drop_bar = passengers_data(frm, pick=False, height=300)
    return pick_bar, drop_bar 
