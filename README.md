# NYC taxi data from january, 2015

Original dataset URL: __http://s3.amazonaws.com/datashader-data/nyc_taxi.zip__
Renamed columns, dropped dependent columns (pick and dropoff hours). Saved
in `parquet` format.

Some plots of data by days and hours, histograms of ride distances and
times, pick and dropoff locations on map, selection on map and selection
plots.

It is considered for workshop at PyCon SK 2024. We need to choose between
this (`marimo`) and `panel` application (repository
__https://github.com/misolietavec/nyc_taxi_panel)__ 

Modules needed: `marimo, plotly, polars, fastparquet`.
