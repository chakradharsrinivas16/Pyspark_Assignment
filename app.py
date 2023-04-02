from pyspark.sql.types import *
from pyspark.sql.functions import *
from flask import Flask, jsonify
from dataframe import df
from pyspark.sql.functions import sum

print("Below is the data after cleaning and creation of dataframe :-\n")
df.show(36)
app = Flask(__name__)
@app.route('/')
def home():
    return jsonify({'/most_affected_state': "Most affected state among all the states ( total death/total covid cases)",
                    '/least_affected_state': "Least affected state among all the states ( total death/total covid cases)",
                    '/highest_covid_cases': "State with highest covid cases.",
                    '/least_covid_cases': "State with least covid cases.",
                    '/total_cases': "Total cases.",
                    '/most_efficient_state':"State that handled the covid most efficiently( total recovery/ total covid cases).",
                    '/least_efficient_state': "State that handled the covid least efficiently( total recovery/ total covid cases).",
                    })

@app.route('/most_affected_state')
def get_most_affected_state():
    most_affected_state=df.sort((df.death.cast("Long")/df.confirm.cast("Long")).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most_affected_state': most_affected_state})

@app.route('/least_affected_state')
def get_least_affected_state():
    least_affected_state=df.sort((df.death.cast("Long")/df.confirm.cast("Long"))).select(col("state")).collect()[0][0]
    return jsonify({'least_affected_state': least_affected_state})

@app.route('/highest_covid_cases')
def get_highest_covid_cases():
    get_highest_covid_cases=df.sort((df.confirm).cast("Long").desc()).select(col("state")).collect()[0][0]
    return jsonify({'get_highest_covid_cases':get_highest_covid_cases})

@app.route('/least_covid_cases')
def get_least_covid_cases():
    get_least_covid_cases=df.sort(df.confirm.cast("Long")).select(col("state")).collect()[0][0]
    return jsonify({'get_least_covid_cases':get_least_covid_cases})

@app.route('/total_cases')
def get_total_cases():
    total_cases=df.select(sum(df.confirm).alias("Total cases")).collect()[0][0]
    return jsonify({'Total Cases':total_cases})
    
@app.route('/most_efficient_state')
def get_most_efficient_state():
    most_efficient_state=df.sort((df.cured.cast("Long")/df.total.cast("Long")).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most efficient_state':most_efficient_state})

@app.route('/least_efficient_state')
def get_least_efficient_state():
    least_efficient_state=df.sort((df.cured.cast("Long")/df.total.cast("Long")).asc()).select(col("state")).collect()[0][0]
    return jsonify({'least efficient_state':least_efficient_state})

if __name__ == '__main__':
    app.run(debug=True)