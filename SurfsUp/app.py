# Import the dependencies.
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################

# reflect an existing database into a new model
engine = create_engine("sqlite:///resources/hawaii.sqlite")

# reflect the tables
Base = automap_base()
Base.prepare(autoload_with=engine)

# Save references to each table
Station = Base.classes.station
Measurement = Base.classes.measurement

# Create our session (link) from Python to the DB
    #  Code to do so is below, but flask was throwing error if i did not open the 
    # session in each route instead
# session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

def calculate_dates():
    """ This function calculates the most recent date of a data set and 
        then returns that date along with a date one year earlier

        Parameters: 
        None

        Returns:
        max_date(datetime): Most recent date
        min_date(datetime): Date one year earlier

    """
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query for all measurement dates and find max, or more recent dates
    date_finder = session.query(Measurement.date)
    date_finder_max = max(date_finder)
    
    # convert to date time
    max_date = dt.datetime.strptime(date_finder_max[0], "%Y-%m-%d" )

    # Calculate the date one year from the last date in data set.
    min_date = max_date - dt.timedelta(days=366)
    
    # Close session we opened at beginning of function
    session.close()

    return max_date, min_date

@app.route("/")
def home():
    # Home page which dictates available routes
    return (
        f"Welcome to my Hawaii Vacation API!<br/> <br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date  <br/>"
        f"/api/v1.0/start_date/end_date<br/>"
        f"(Note: dates must be in YYYY-DD-MM format)"
    )
    

@app.route("/api/v1.0/precipitation")
def precip():
    # Instructions:
        # Convert the query results from your precipitation analysis 
        # (i.e. retrieve only the last 12 months of data) to a dictionary 
        # using date as the key and prcp as the value.


    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # run calculate_dates custom function to set most recent date and 
    # date one year prior to that, inclusive
    max_date, min_date = calculate_dates()

    # Perform a query to retrieve the data and precipitation scores
    prec_data = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date <= max_date).\
        filter(Measurement.date >= min_date).all()

    # Create a dictionary from the row data to jsonify
    prec_results = []
    for date, prcp in prec_data:
        prec_dict = {}
        prec_dict[date] = prcp
        prec_results.append(prec_dict)

    # Close session we opened at beginning of function
    session.close()

    #Return the JSON representation of your dictionary
    return jsonify(prec_results)

@app.route("/api/v1.0/stations")
def stations():
    # Instructions:
        # Return a JSON list of stations from the dataset.


    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query for station numbers and precipitation data
    station_data = session.query(Measurement.station, Measurement.prcp).\
    group_by(Measurement.station).all()

   # Create a dictionary from the row data to jsonify
    station_results = []
    for station, name in station_data:
        stat_dict = {}
        stat_dict['station'] = station
        station_results.append(stat_dict)

    # Close session we opened at beginning of function
    session.close()

    #Return the JSON representation of your dictionary
    return jsonify(station_results)

@app.route("/api/v1.0/tobs")
def tobs():
    # Instructions:
        # Query the dates and temperature observations of the most-active station 
        # for the previous year of data.


    # Create our session (link) from Python to the DB
    session = Session(engine)

    # run calculate_dates custom function to set most recent date and 
    # date one year prior to that, inclusive
    max_date, min_date = calculate_dates()

    # returns first item of first row, which is the name of the most active station
    # since query was listed in descending order
    station_data = session.query(Measurement.station).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).first()   
    most_active = station_data[0]
    
    # run second query which will filter by the most active station during the last year 
    most_active_data = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.station == most_active).\
        filter(Measurement.date <= max_date).\
        filter(Measurement.date >= min_date).\
        all()



    # Create a dictionary from the row data to jsonify
    tobs_results = []
    for date, tobs in most_active_data:
        tobs_dict = {}
        tobs_dict[date] = tobs
        tobs_results.append(tobs_dict)

    # Close session we opened at beginning of function
    session.close()
   
    # Return a JSON list of temperature observations for the previous year.
    return jsonify(tobs_results)

@app.route("/api/v1.0/<start_date>")
def start_return(start_date):
    """dates must be entered in "YYYY-MM-DD" format"""
    # Instructions:
        # Return a JSON list of the minimum temperature, the average temperature, 
        # and the maximum temperature for a specified start or start-end range.
        #
        # For a specified start, calculate TMIN, TAVG, and TMAX for all the dates 
        # greater than or equal to the start date.


    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query for the min, max, and average of temperature data after a user-provided date, inclusive
    results = session.query(
        func.min(Measurement.tobs),\
        func.avg(Measurement.tobs),\
        func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date)

    # Create a dictionary from the row data to jsonify
    start_results = []
    for tmin, tavg, tmax in results:
        start_dict = {}
        start_dict['tmin'] = tmin
        start_dict['tavg'] = tavg
        start_dict['tmax'] = tmax
        start_results.append(start_dict)

    # Close session we opened at beginning of function
    session.close()

    #Return the JSON representation of your dictionary
    return jsonify(start_results)

@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    # Instructions:
    # Return a JSON list of the minimum temperature, the average temperature, 
    # and the maximum temperature for a specified start or start-end range.
    #
    # For a specified start date and end date, calculate TMIN, TAVG, and TMAX 
    # for the dates from the start date to the end date, inclusive.


    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query for the min, max, and average of temperature data between two user provided dates, inclusive
    results = session.query(
        func.min(Measurement.tobs),\
        func.avg(Measurement.tobs),\
        func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).\
        filter(Measurement.date <= end)
            
    # Create a dictionary from the row data to jsonify
    stend_results = []
    for tmin, tavg, tmax in results:
        stend_dict = {}
        stend_dict['tmin'] = tmin
        stend_dict['tavg'] = tavg
        stend_dict['tmax'] = tmax
        stend_results.append(stend_dict)

    # Close session we opened at beginning of function
    session.close()

    #Return the JSON representation of your dictionary
    return jsonify(stend_results)

if __name__ == "__main__":
    app.run(debug=True)