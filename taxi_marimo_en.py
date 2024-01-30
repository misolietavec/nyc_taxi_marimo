import marimo

__generated_with = "0.1.88"
app = marimo.App(width="full")


@app.cell
def __():
    import marimo as mo
    return mo,


@app.cell
def __():
    import polars as pl
    import plotly.express as px
    import numpy as np
    from data_functions_en import make_graphs, static_graphs, total_graphs
    return make_graphs, np, pl, px, static_graphs, total_graphs


@app.cell
def __(make_graphs, pl):
    nsample = 155000
    _dflocal = 'data/nyc_taxi155k.parq'
    df = pl.read_parquet(_dflocal)
    meanloc = [df['pick_lat'].mean(), df['pick_lon'].mean()]
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
    day_choose = mo.ui.slider(start=1, stop=31, value=14, debounce=True, label='Day')
    hour_choose = mo.ui.slider(start=0, stop=23, value=11, debounce=True, label='Hour')
    direction = mo.ui.radio(options=['Pickup','Dropoff'], value='Pickup', label='Direction', inline=True)
    day_or_hour = mo.ui.radio(options=['By days','By hours'], value='By days', inline=True)
    map_selection = mo.ui.checkbox(label='Enable box selection')
    map_day_choose = mo.ui.slider(start=1, stop=31, value=14, debounce=True, label='Day for selection')
    nbins = mo.ui.slider(start=10, stop=120, value=20, label='Bins number')
    return (
        day_choose,
        day_or_hour,
        direction,
        hour_choose,
        map_day_choose,
        map_selection,
        nbins,
    )


@app.cell
def __(day_choose, dfdays, direction, mo):
    def _view_hourly():
        if direction.value == 'Pickup':
            return dfdays[day_choose.value]['pick_graph']
        return dfdays[day_choose.value]['drop_graph']
    _hourly_info = mo.md(f"Day: {day_choose.value}")
    hourly = mo.vstack([mo.hstack([direction, day_choose, _hourly_info], justify='center'), 
                        _view_hourly()], align='center')
    return hourly,


@app.cell
def __(day_or_hour, mo, static_days, static_hours):
    def _view_totals():
        return static_days if day_or_hour.value == 'By days' else static_hours
    totals = mo.vstack([day_or_hour, _view_totals()], align='center')
    return totals,


@app.cell
def __(df, mo, nbins, np, pl, px):
    def _view_distances(): 
        y, x = np.histogram(df['distance'], bins=nbins.value, range=(0, 8))
        x = (x[0:-1] + x[1:]) / 2
        df_hist = pl.DataFrame({'x': x, 'y': y})
        return px.bar(data_frame=df_hist, x='x', y='y', 
                      barmode='group', labels={'x': 'Distance', 'y': 'count'}, width=900, height=350)


    def _view_rtimes():
        y, x = np.histogram(df['rtime'], bins=nbins.value, range=(0, 45)) # min.
        x = (x[0:-1] + x[1:]) / 2  # centers of intervals
        df_hist = pl.DataFrame({'x': x, 'y': y})
        return px.bar(data_frame=df_hist, x='x', y='y', 
                      barmode='group', labels={'x': 'Ride time (min.)', 'y': 'count'}, width=900, height=350)
    dist_and_times = mo.vstack([mo.hstack([nbins, mo.md(f"Nbins: {nbins.value}")], justify='center'),
                                _view_distances(), _view_rtimes()])
    return dist_and_times,


@app.cell
def __(day_choose, df, direction, hour_choose, meanloc, mo, px):
    def view_map():
        is_pick = (direction.value == 'Pickup')
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
    dist_and_times,
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
        # Taxi in New York City
        ### Data from january 2015, sample of {nsample} records.""")
    _local_data =(mapplot.ranges != {}) and map_selection.value
    # add plot for dropoff
    if _local_data:
        _mapranges = mapplot.ranges['mapbox']
        lon_min, lat_max = _mapranges[0]
        lon_max, lat_min = _mapranges[1]
        df_ranges = df.filter((lat_min < pl.col('pick_lat')) & (pl.col('pick_lat') < lat_max) &
                              (lon_min < pl.col('pick_lon')) & (pl.col('pick_lon') < lon_max) &
                              (pl.col('pick_day') == map_day_choose.value))

    _local_plot = total_graphs(df_ranges, pick=True, what=['total_pass', 'total_rides', 'total_fare']) if _local_data else mo.md('## Nothing selected or selection not enabled')
    _sel_info = mo.md(f"Day: {map_day_choose.value}")
    _loc_widgets = mo.hstack([map_day_choose, _sel_info], justify='center', widths=[100, 50])
    _selection_body = mo.vstack([_loc_widgets, _local_plot]) if _local_data else mo.vstack([_local_plot])

    _map_info = mo.md(f"Day: {day_choose.value}, Hour: {hour_choose.value}")
    _maps = mo.vstack([mo.hstack([direction, day_choose, hour_choose, map_selection], justify='center'), 
                      _map_info, mapplot], align='center')
    _tabs = mo.tabs({'Day plots': hourly, 'Monthly plots': totals, 'Histograms': dist_and_times,
                     'Locations on map': _maps, 'Selection graphs': _selection_body})

    app_tabs = mo.vstack([_main_title, _tabs], align='stretch') 
    app_tabs
    return app_tabs, df_ranges, lat_max, lat_min, lon_max, lon_min


if __name__ == "__main__":
    app.run()
