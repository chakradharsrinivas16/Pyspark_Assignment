# Importing the required imports
from pyspark.sql.types import *
from pyspark.sql.functions import *
from flask import Flask, jsonify
from dataframe import df # Fecthing the dataframe created in other file.
from pyspark.sql.functions import sum
from datetime import datetime
import os
desktop = os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop')+"/results" 


 
print("Below is the data after cleaning and creation of dataframe :-\n")
df.show() # Displaying the above fetched dataframe
app = Flask(__name__) # creating a app from flask

@app.route('/') # defing the things to happen on home path
def home():
    # returning the jsonfied index
    return jsonify({'/most_affected_state': "Most affected state among all the states ( total death/total covid cases)",
                    '/least_affected_state': "Least affected state among all the states ( total death/total covid cases)",
                    '/highest_covid_cases': "State with highest covid cases.",
                    '/least_covid_cases': "State with least covid cases.",
                    '/total_cases': "Total cases.",
                    '/most_efficient_state':"State that handled the covid most efficiently( total recovery/ total covid cases).",
                    '/least_efficient_state': "State that handled the covid least efficiently( total recovery/ total covid cases).",
                    '/getcsvfile':"To export data to csv file at given path"
                    })

@app.route('/getcsvfile') # defing the things to happen on /getcsvfile path
def getcsvfile():
    df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(desktop+str(datetime.now()))
    return jsonify({"Message":"Results stored succesfully to "+desktop+str(datetime.now())+"' path"})

@app.route('/most_affected_state') # defing the things to happen on /most_affected_state
def get_most_affected_state():
    # Sorting the data frame as per given criteria in descending and selecting the top most record and state column
    most_affected_state=df.sort((df.death/df.confirm).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most_affected_state': most_affected_state})  # returning the jsonfied response

@app.route('/least_affected_state') # defing the things to happen on /least_affected_state
def get_least_affected_state():
    # Sorting the data frame as per given criteria in ascending and selecting the top most record and state column
    least_affected_state=df.sort((df.death/df.confirm)).select(col("state")).collect()[0][0]
    return jsonify({'least_affected_state': least_affected_state}) # returning the jsonfied response

@app.route('/highest_covid_cases') # defing the things to happen on /highest_covid_cases
def get_highest_covid_cases():
    # Sorting the data frame as per given criteria in descneding and selecting the top most record and state column
    highest_covid_cases=df.sort((df.confirm).desc()).select(col("state")).collect()[0][0]
    return jsonify({'get_highest_covid_cases':highest_covid_cases}) # returning the jsonfied response

@app.route('/least_covid_cases') # defing the things to happen on /least_covid_cases
def get_least_covid_cases():
    # Sorting the data frame as per given criteria in acending and selecting the top most record and state column
    least_covid_cases=df.sort(df.confirm).select(col("state")).collect()[0][0]
    return jsonify({'get_least_covid_cases':least_covid_cases}) # returning the jsonfied response

@app.route('/total_cases') # defing the things to happen on /total_cases
def get_total_cases():
    # Suming the data frame as per given criteria and selecting the sum.
    total_cases=df.select(sum(df.total).alias("Total cases")).collect()[0][0]
    return jsonify({'Total Cases':total_cases}) # returning the jsonfied response
    
@app.route('/most_efficient_state') # defing the things to happen on /most_efficient_state
def get_most_efficient_state():
    # Sorting the data frame as per given criteria in descending and selecting the top most record and state column
    most_efficient_state=df.sort((df.cured/df.confirm).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most efficient_state':most_efficient_state}) # returning the jsonfied response

@app.route('/least_efficient_state') # defing the things to happen on /least_efficient_state
def get_least_efficient_state():
    # Sorting the data frame as per given criteria in ascending and selecting the top most record and state column
    least_efficient_state=df.sort((df.cured/df.confirm).asc()).select(col("state")).collect()[0][0]
    return jsonify({'least efficient_state':least_efficient_state}) # returning the jsonfied response

if __name__ == '__main__':
    app.run(debug=True) #running the app
