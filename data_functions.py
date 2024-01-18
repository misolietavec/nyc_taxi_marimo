import polars as pl
import plotly.express as px
import os, pickle


def passengers_data(frm, pick=True, height=400):
    column = 'pick_dt' if pick else 'drop_dt'
    ylabel = 'Nástupy' if pick else 'Výstupy'
    by_hours = frm.group_by(frm[column].dt.hour()) # pick_hour or drop_hour
    pgs = by_hours.agg(pl.col('passengers').sum().alias('cestujúci'))
    pgs = pgs.with_columns(by_hours.agg(pl.col('fare').count().alias('jazdy').cast(pl.Float32)))
    pgs = pgs.with_columns(by_hours.agg(pl.col('fare').sum().alias('peniaze')))    
    max_jaz, max_pas, max_far = pgs['jazdy'].max(), pgs['cestujúci'].max(), pgs['peniaze'].max()
    coeff = max(max_jaz, max_pas) / max_far
    pgs = pgs.with_columns(pl.col('peniaze') * coeff)
    trzba = pgs['peniaze'].sum()
    scalestr = f"Peniaze vynásobené {coeff:.3f}, tržba {trzba:.2f}"
    
    graf = px.bar(pgs, x=column, y=['cestujúci', 'jazdy', 'peniaze'], 
                  barmode='group', height=height, width=750, # title=scalestr, 
                  labels={column: 'hodiny', 'value': ylabel, 'variable': 'premenné'})
    # graf.update_layout(margin={'t': 40, 'b': 5})
    return graf


# Predrobime grafy podla dni a ulozime ich - vykona sa to len raz
def make_graphs(df):
    if not os.path.isfile('data/dfdays.pic'):
        dfdays = {}
        for day in range(1, 32):
            pick_df = df.filter(pl.col('pick_dt').dt.day() == day) 
            drop_df = df.filter(pl.col('drop_dt').dt.day() == day)
            dfdays[day] = {}
            dfdays[day]['pick_graph'] = passengers_data(pick_df) 
            dfdays[day]['drop_graph'] = passengers_data(drop_df, pick=False)
            pickle.dump(dfdays, open('data/dfdays.pic','wb'))
    else:
        dfdays = pickle.load(open('data/dfdays.pic','rb'))
    return dfdays


# Grafy - nastup a vystup podla hodin (jazdy, pocet prevezenych, trzby), celkovo
def static_graphs(frm):
    pick_bar = passengers_data(frm, height=300)
    drop_bar = passengers_data(frm, pick=False, height=300)
    return pick_bar, drop_bar 
