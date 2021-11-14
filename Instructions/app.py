# Python SQL toolkit and Object Relational Mapper
from os import X_OK
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

import flask
import pandas as pd
import numpy as np
import datetime as dt
from flask import Flask, jsonify

# create engine to hawaii.sqlite
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# View all of the classes that automap found
Base.classes.keys()

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

app = Flask(__name__)

@app.route("/")
def home():
    print("test")
    return (
        f"Available Routes:<br/>"
        f"<br/>"
        f"List of Precipitation for the Past Year<br/>"
        f"/api/v1.0/precipitation:<br/>"
        f"<br/>"
        f"Most Active Stations<br/>"
        f"/api/v1.0/stations:<br/>"
        f"<br/>"
        f"Max, Min and Average Temp for Most Active Station<br/>"
        f"/api/v1.0/tobs:<br/>"
        f"<br/>"
        f"Start Date Temperature Max, Min and Average<br/>"
        f"/api/v1.0/<start>:<br/>"
        f"<br/>"
        f"Start and End Date Temperature Max, Min and Average<br/>"
        f"/api/v1.0/<start>/<end>")


@app.route("/api/v1.0/precipitation")
def precipitation():
#Convert the query results to a dictionary using date as the key and prcp as the value. Return the JSON representation of your dictionary.
   
    start_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first().date
    end_date = dt.date(2017, 8, 23) - dt.timedelta(days=365)

    session.query(Measurement.date, Measurement.prcp).\
    order_by(Measurement.date.desc()).all()

    precipitation_info = session.query(Measurement.date, Measurement.prcp).\
    filter(Measurement.date >= end_date).\
    order_by(Measurement.date.asc()).all()

    precipitation_df = pd.DataFrame(precipitation_info, columns=['Date', 'Precipitation'])
    precipitation_df.set_index('Date', inplace=True, )
    prcp_date = precipitation_df.sort_values(by='Date', ascending=True)

    #convert to a dictionary
    precipitation = []

    for index, result in prcp_date.iterrows():
        row = {}
        row["Date"] = index
        row["prcp"] = result[0]

        precipitation.append(row)
        
    session.close
    return jsonify(precipitation)

@app.route("/api/v1.0/stations")
def stations():
    stations = session.query(Measurement.station, func.count(Measurement.station)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).all()

    station_list = []

    for index, result in stations:
        row = {}
        row["station"] = stations[0][0]
        station_list.append(row)
    
    session.close
    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    end_date = dt.date(2017, 8, 23) - dt.timedelta(days=365)

    stations = session.query(Measurement.station, func.count(Measurement.station)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).all()

    active_station_id = stations[0][0]

    active_station_temp = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
        filter (active_station_id == Measurement.station).all()

    temp_info = session.query(Measurement.station, Measurement.date, Measurement.tobs).\
        filter(Measurement.station == active_station_id).\
        filter(Measurement.date >= end_date).\
        order_by(Measurement.date.asc()).all()

    station_tob = []

    for result in temp_info:
        row = {}
        row["station"] = result[0]
        row["date"] = result[1]
        row["tobs"] = result[2]
        station_tob.append(row)

    session.close
    return jsonify(station_tob)

@app.route("/api/v1.0/<start>")
def start(start, name=None):

    start_date = dt.date(2017, 8, 23) - dt.timedelta(days=365)

    temp_info = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
        filter(Measurement.date >= start_date).\
        order_by(Measurement.date.asc()).all()
    
    start_tob = []

    for result in temp_info:
        row = {}
        row["Minimum Temperature"] = result[0]
        row["Maximum Temperature"] = result[1]
        row["Average Temperature"] = result[2]
        start_tob.append(row)

    session.close
    return jsonify(start_tob)

@app.route("/api/v1.0/<start>/<end>")
def end(start, end, name=None):

    start_date = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    end_date = dt.date(2019, 8, 23) - dt.timedelta(days=365)

    end_info = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
        filter(Measurement.date >= start_date).\
        filter(Measurement.date <= end_date).\
        order_by(Measurement.date.asc()).all()
    
    end_tob = []

    for result in end_info:
        row = {}
        row["Minimum Temperature"] = result[0]
        row["Maximum Temperature"] = result[1]
        row["Average Temperature"] = result[2]
        end_tob.append(row)

    session.close
    return jsonify(end_tob)

if __name__ == "__main__":
    app.run(debug=True)