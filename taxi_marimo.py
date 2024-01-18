import marimo

__generated_with = "0.1.78"
app = marimo.App(width="full")


@app.cell
def __():
    import marimo as mo
    return mo,


@app.cell
def __():
    import polars as pl
    import plotly.express as px
    import os
    from data_functions import make_graphs, static_graphs
    return make_graphs, os, pl, px, static_graphs


@app.cell
def __(make_graphs, os, pl):
    nsample = 600000
    _dflocal = 'data/nyc_taxi.parq'
    _exists_local = os.path.isfile(_dflocal)
    _dfname = _dflocal if _exists_local else 'https://feelmath.eu/Download/nyc_taxi.parq'
    df = pl.read_parquet(_dfname).sample(nsample)
    if not _exists_local:
        df.write_parq(_dflocal)
    meanloc = [df['pick_lat'].mean(), df['pick_lon'].mean()]
    dfdays = make_graphs(df)
    return df, dfdays, meanloc, nsample


@app.cell
def __(df, mo, static_graphs):
    _pick_bar, _drop_bar = static_graphs(df)
    static_plots = mo.vstack([_pick_bar, _drop_bar])
    return static_plots,


@app.cell
def __(mo):
    day_choose = mo.ui.slider(start=1, stop=31, value=14, debounce=True, label='Deň')
    hour_choose = mo.ui.slider(start=0, stop=23, value=11, debounce=True, label='Hodina')
    smer = mo.ui.radio(options=['Nástup','Výstup'], value='Nástup', label='Smer', inline=True)
    return day_choose, hour_choose, smer


@app.cell
def __(day_choose, dfdays, mo, smer):
    def _view_hourly():
        if smer.value == 'Nástup':
            return dfdays[day_choose.value]['pick_graph']
        return dfdays[day_choose.value]['drop_graph']
    hourly = mo.vstack([mo.hstack([smer, day_choose], justify='center'), _view_hourly()], align='center')
    return hourly,


@app.cell
def __(day_choose, df, hour_choose, meanloc, mo, px, smer):
    def view_map():
        is_pick = (smer.value == 'Nástup')
        col_dt = 'pick_dt' if is_pick else 'drop_dt'
        df_filtered = df.filter((df[col_dt].dt.day() == day_choose.value) & 
                                (df[col_dt].dt.hour() == hour_choose.value))
        col_lat = 'pick_lat' if is_pick else 'drop_lat'
        col_lon = 'pick_lon' if is_pick else 'drop_lon'
        lat, lon = df_filtered[col_lat], df_filtered[col_lon]
        center_lat, center_lon = [lat.mean(), lon.mean()] if len(lat) else meanloc
        fig = px.scatter_mapbox(df_filtered, lat=col_lat, lon=col_lon,
                                color_discrete_sequence=["green"],
                                mapbox_style="open-street-map", zoom=10, width=750, height=500)
        fig.update_traces(marker={"size": 4})
        fig.update_layout(margin={'t': 25}, hovermode=False)
        return mo.ui.plotly(fig)
    return view_map,


@app.cell
def __(
    day_choose,
    hour_choose,
    hourly,
    mo,
    nsample,
    smer,
    static_plots,
    view_map,
):
    _nadpis_celkovy = mo.md(
        f"""
        # Taxi v New Yorku
        ### Dáta z januára 2015, vzorka {nsample} zápisov, celkovo je ich vyše 11 mil.""")
    mapplot = view_map()
    _maps = mo.vstack([mo.hstack([smer, day_choose, hour_choose], justify='center'), mapplot], align='center')
    _tabs = mo.tabs({'Grafy podľa dní': hourly, 'Grafy celkové': static_plots,
                   'Miesta na mape': _maps})
    mo.vstack([_nadpis_celkovy, _tabs], align='stretch')
    return mapplot,


@app.cell
def __(mapplot):
    mapplot.value[:5]
    return


if __name__ == "__main__":
    app.run()
