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
    from data_functions_en import make_graphs, static_graphs, passengers_data
    return make_graphs, os, passengers_data, pl, px, static_graphs


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
    day_choose = mo.ui.slider(start=1, stop=31, value=14, debounce=True, label='Day')
    hour_choose = mo.ui.slider(start=0, stop=23, value=11, debounce=True, label='Hour')
    direction = mo.ui.radio(options=['Pickup','Dropoff'], value='Pickup', label='Direction', inline=True)
    map_day_choose = mo.ui.slider(start=1, stop=31, value=14, debounce=True, label='Day for selection')
    return day_choose, direction, hour_choose, map_day_choose


@app.cell
def __(day_choose, dfdays, direction, mo):
    def _view_hourly():
        if direction.value == 'Pickup':
            return dfdays[day_choose.value]['pick_graph']
        return dfdays[day_choose.value]['drop_graph']
    hourly = mo.vstack([mo.hstack([direction, day_choose], justify='center'), _view_hourly()], align='center')
    return hourly,


@app.cell
def __(day_choose, df, direction, hour_choose, meanloc, mo, px):
    def view_map():
        is_pick = (direction.value == 'Pickup')
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
    mapplot,
    mo,
    nsample,
    passengers_data,
    pl,
    static_plots,
):
    _main_title = mo.md(
        f"""
        # Taxi in New York City
        ### Data from january 2015, sample of {nsample} records.""")
    _local_data = mapplot.ranges != {}

    # add plot for dropoff
    if _local_data:
        _mapranges = mapplot.ranges['mapbox']
        lon_min, lat_max = _mapranges[0]
        lon_max, lat_min = _mapranges[1]
        df_ranges = df.filter((lat_min < pl.col('pick_lat')) & (pl.col('pick_lat') < lat_max) &
                              (lon_min < pl.col('pick_lon')) & (pl.col('pick_lon') < lon_max) &
                              (pl.col('pick_dt').dt.day() == day_choose.value))

    _local_plot = passengers_data(df_ranges, pick=True, what=['passengers', 'rides']) if _local_data else mo.md('# No selection')
    _selection_body = mo.vstack([map_day_choose, _local_plot]) if _local_data else mo.vstack([_local_plot])

    _maps = mo.vstack([mo.hstack([direction, day_choose, hour_choose], justify='center'), 
                       mapplot], align='center')
    _tabs = mo.tabs({'Day plots': hourly, 'Summary plots': static_plots,
                     'Locations on map': _maps, 'Selection graphs': _selection_body})

    app_tabs = mo.vstack([_main_title, _tabs], align='stretch') 
    app_tabs
    return app_tabs, df_ranges, lat_max, lat_min, lon_max, lon_min


if __name__ == "__main__":
    app.run()
