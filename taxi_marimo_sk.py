import marimo

__generated_with = "0.1.79"
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
    from data_functions_sk import make_graphs, static_graphs, total_graphs
    return make_graphs, os, pl, px, static_graphs, total_graphs


@app.cell
def __(make_graphs, pl):
    nsample = 155000
    _dflocal = 'data/nyc_taxi155k.parq'
    # _exists_local = os.path.isfile(_dflocal)
    # _dfname = _dflocal if _exists_local else 'https://feelmath.eu/Download/nyc_taxi.parq'
    df = pl.read_parquet(_dflocal) # .sample(nsample)
    # if not _exists_local:
    #    df.write_parq(_dflocal)
    meanloc = [df['pick_lat'].mean(), df['pick_lon'].mean()]
    df = df.with_columns([(df['pick_dt'].dt.day().alias('pick_day')), 
                          (df['pick_dt'].dt.hour().alias('pick_hour')),
                          (df['drop_dt'].dt.day().alias('drop_day')),
                          (df['drop_dt'].dt.hour().alias('drop_hour'))])\
                        .drop(['pick_dt', 'drop_dt'])
    dfdays = make_graphs(df, create=False)
    return df, dfdays, meanloc, nsample


@app.cell
def __(df, mo, static_graphs):
    _pick_days, _drop_days, _pick_hours, _drop_hours = static_graphs(df)
    static_days = mo.vstack([_pick_days, _drop_days])
    static_hours = mo.vstack([_pick_hours, _drop_hours])
    return static_days, static_hours


@app.cell
def __(mo):
    day_choose = mo.ui.slider(start=1, stop=31, value=14, debounce=True, label='Deň')
    hour_choose = mo.ui.slider(start=0, stop=23, value=11, debounce=True, label='Hodina')
    direction = mo.ui.radio(options=['Nástup','Výstup'], value='Nástup', label='Smer', inline=True)
    map_selection = mo.ui.checkbox(label='Umožniť výber')
    map_day_choose = mo.ui.slider(start=1, stop=31, value=14, debounce=True, label='Deň pre výber')
    day_or_hour = mo.ui.radio(options=['Podľa dní','Podľa hodín'], value='Podľa dní', inline=True)
    return (
        day_choose,
        day_or_hour,
        direction,
        hour_choose,
        map_day_choose,
        map_selection,
    )


@app.cell
def __(day_choose, dfdays, direction, mo):
    def _view_hourly():
        if direction.value == 'Nástup':
            return dfdays[day_choose.value]['pick_graph']
        return dfdays[day_choose.value]['drop_graph']
    _hourly_info = mo.md(f"Deň: {day_choose.value}")
    hourly = mo.vstack([mo.hstack([direction, day_choose, _hourly_info], justify='center'), 
                        _view_hourly()], align='center')
    return hourly,


@app.cell
def __(day_or_hour, mo, static_days, static_hours):
    def _view_totals():
        return static_days if day_or_hour.value == 'Podľa dní' else static_hours
    totals = mo.vstack([day_or_hour, _view_totals()], align='center')
    return totals,


@app.cell
def __(day_choose, df, direction, hour_choose, meanloc, mo, px):
    def view_map():
        is_pick = (direction.value == 'Nástup')
        col_day = 'pick_day' if is_pick else 'drop_day'
        col_hour = 'pick_hour' if is_pick else 'drop_hour'
        df_filtered = df.filter((df[col_day] == day_choose.value) & 
                                (df[col_hour] == hour_choose.value))
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
    mapplot = view_map()
    return mapplot, view_map


@app.cell
def __(
    day_choose,
    df,
    direction,
    hour_choose,
    hourly,
    map_day_choose,
    map_selection,
    mapplot,
    mo,
    nsample,
    pl,
    total_graphs,
    totals,
):
    _main_title = mo.md(
        f"""
        # Taxi v New Yorku
        ### Dáta z januára 2015, výber {nsample} záznamov.""")
    _local_data =(mapplot.ranges != {}) and map_selection.value
    # add plot for dropoff
    if _local_data:
        _mapranges = mapplot.ranges['mapbox']
        lon_min, lat_max = _mapranges[0]
        lon_max, lat_min = _mapranges[1]
        df_ranges = df.filter((lat_min < pl.col('pick_lat')) & (pl.col('pick_lat') < lat_max) &
                              (lon_min < pl.col('pick_lon')) & (pl.col('pick_lon') < lon_max) &
                              (pl.col('pick_day') == map_day_choose.value))

    _local_plot = total_graphs(df_ranges, pick=True, what=['Cestujúci', 'Jazdy', 'Platby']) if _local_data else mo.md('## Nič nie je vybrané, alebo výber nie je umožnený')
    _sel_info = mo.md(f"Deň: {map_day_choose.value}")
    _loc_widgets = mo.hstack([map_day_choose, _sel_info], justify='center', widths=[100, 50])
    _selection_body = mo.vstack([_loc_widgets, _local_plot]) if _local_data else mo.vstack([_local_plot])

    _map_info = mo.md(f"Deň: {day_choose.value}, Hodina: {hour_choose.value}")
    _maps = mo.vstack([mo.hstack([direction, day_choose, hour_choose, map_selection], justify='center'), 
                      _map_info, mapplot], align='center')
    _tabs = mo.tabs({'Grafy po dňoch': hourly, 'Grafy celkové': totals,
                     'Polohy na mape': _maps, 'Grafy pre výber': _selection_body})

    app_tabs = mo.vstack([_main_title, _tabs], align='stretch') 
    app_tabs
    return app_tabs, df_ranges, lat_max, lat_min, lon_max, lon_min


if __name__ == "__main__":
    app.run()
