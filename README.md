# COVID WEB

This is a simple web server for building various custom data graphs of COVID-19 data.

## History

I have been creating various COVID-19 data graphs on an ad-hoc basis
using several different data sources and python tools in a jupyter notebook.
It's a little tiresome to constantly rerun these scripts, so I figured it
might be time to formalize things a bit and build a proper web app.

## Running

Make sure your redis server is up and running. Include it's uri in config.py.
For MacOS, you can issue the command ```brew services start redis```.
On linux, consider using docker.

For development, it's handy to include a source watcher while running the server
```
gunicorn --reload server:app
```

## Data sources

Detail | Source | Link
-- | -- | ---
County | New York Times | https://github.com/nytimes/covid-19-data
State | Covid tracking project | https://covidtracking.com
International | Our World in Data | https://ourworldindata.org/coronavirus-source-data

## Architecture

The client is presented with a single dynamic html page. Selections are
presented in DHTML controlled entirely in javascript implemented with jquery.

## Vega-Embed

A modified and limited version of Vega-Embed is included. This version
was modified from the original TypeScript and is not recommended for
any other application.

See: https://github.com/vega/vega-embed/blob/master/LICENSE

## Technology

This web app uses a python3 backend, powered by gunicorn/flask, with an 
old-fashioned templated front-end. Graphs are rendered on the client using 
vega-lite. Data is cached on the backend using a redis server.