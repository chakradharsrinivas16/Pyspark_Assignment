# Pyspark_Assignment

## Dataframe.py

This file contains basically, script to fecth the data from API and clean it to make it ready for query use. This involes steps like creating rdd and dataframe and this code finally returns a dataframe which is used for querying.
The below are code snippets and their explanations -

```
def get_data():
	url = "https://covid-19-india2.p.rapidapi.com/details.php"
	headers = {
		"X-RapidAPI-Key": KEY,
		"X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
	}
	response = requests.request("GET", url, headers=headers)
	return response
```

The get_data() function sends an HTTP GET request to the "https://covid-19-india2.p.rapidapi.com/details.php" endpoint using the requests library. It sets headers with the API key and host. The function returns the response object.

```
def clean_data(df):
    df=df.drop('_corrupt_record') # droping the _corrupt_record column
    df=df.where((df.state.isNotNull()) & (df.state!='')) #fetching records only whose state is not void or null.
    # Casting the columns to desired formats
    df = df.withColumn("confirm",col("confirm").cast("Long")).withColumn("cured",col("cured").cast("Long")).withColumn("death",col("death").cast("Long"))
    # Reaaranging the columns to give a good view
    df=df.select("slno","state","confirm","cured","death","total")
    # Striping state names who has * st their ends
    df=df.withColumn('state', regexp_replace('state', '\*',""))
    return df # returning the cleaned dataframe
```

The clean_data() function accepts a PySpark dataframe as an argument. It drops the _corrupt_record column and filters records where the state column is not null or empty. It then casts the confirm, cured, and death columns to Long type using the withColumn() method. The function then rearranges the columns to make them more readable and strips asterisks from the end of state names using the regexp_replace() method. Finally, it returns the cleaned dataframe.

we wrote a driver code which cn=onnects data and these function onorder to obtain a clean and consice output, the below is the snippet.
```
spark = SparkSession.builder.master('local[*]').getOrCreate() # Creating a spark session
sc = SparkContext.getOrCreate() # creating a Spark context
sc.setLogLevel("ERROR")
response=get_data() # getting the response objet from the above created method
json_rdd = sc.parallelize(response.json().values()) # creating a rdd from the data fetched above
df = spark.read.json(json_rdd) # creating the dataframe for the above created rdd
df=clean_data(df) # cleaning the above created data frame
```

The script creates a SparkSession object using the builder() method and sets the master to local[*], which means to use all available cores on the local machine. It then creates a SparkContext object and sets the log level to ERROR.

The script calls the get_data() function to fetch data from the API and creates an RDD from the response JSON values using the parallelize() method. It then creates a PySpark dataframe from the RDD using the read.json() method and assigns it to the variable df.

Finally, the script calls the clean_data() function to clean the df dataframe and assigns the cleaned dataframe to df.


## APP.py

We imported the above created datafrae into this code file, and made an API using flask and it is as below -

The script creates a Flask application instance named app using the Flask() constructor from the Flask library.
```
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
                    })
```
It then uses the route() decorator to specify the URL route of the function that should handle HTTP requests. In this case, it specifies that the function to handle the root URL should be executed when the client requests the '/' route.
The home() function is defined as the route handler for the '/' route. When the client requests the root URL, it returns a JSON object that provides information about other routes that the user can access. This JSON object provides descriptions of several other routes that can be used to query the COVID-19 data, such as the most affected state, least affected state, highest COVID-19 cases, and so on.

```
@app.route('/most_affected_state') # defing the things to happen on /most_affected_state
def get_most_affected_state():
    # Sorting the data frame as per given criteria in descending and selecting the top most record and state column
    most_affected_state=df.sort((df.death.cast("Long")/df.confirm.cast("Long")).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most_affected_state': most_affected_state})  # returning the jsonfied response

```
This function defines the behavior of the Flask app when the user requests the endpoint "/most_affected_state". It calculates the most affected state among all the states by sorting the DataFrame based on the ratio of total deaths to total COVID-19 cases in each state, and then selecting the state with the highest ratio. It returns a JSON object with the name of the most affected state.

```
@app.route('/least_affected_state') # defing the things to happen on /least_affected_state
def get_least_affected_state():
    # Sorting the data frame as per given criteria in ascending and selecting the top most record and state column
    least_affected_state=df.sort((df.death.cast("Long")/df.confirm.cast("Long"))).select(col("state")).collect()[0][0]
    return jsonify({'least_affected_state': least_affected_state}) # returning the jsonfied response
```
This function handles a GET request to the '/least_affected_state' endpoint. It sorts the 'df' DataFrame based on the ratio of deaths to total confirmed cases in ascending order, selects the state column of the topmost record, and assigns it to the 'least_affected_state' variable. Finally, it returns the state with the least number of deaths per confirmed cases as a JSON object.

```
@app.route('/highest_covid_cases') # defing the things to happen on /highest_covid_cases
def get_highest_covid_cases():
    # Sorting the data frame as per given criteria in descneding and selecting the top most record and state column
    highest_covid_cases=df.sort((df.confirm).cast("Long").desc()).select(col("state")).collect()[0][0]
    return jsonify({'get_highest_covid_cases':highest_covid_cases}) # returning the jsonfied response
```
The get_highest_covid_cases function retrieves the state with the highest number of confirmed COVID-19 cases in India from the Spark dataframe df, sorts the dataframe in descending order based on the number of confirmed cases, selects the state column of the first record, and returns the result as a JSON object. This function is associated with the /highest_covid_cases route in the Flask application, so when a user sends a GET request to this route, the function is executed and returns the result as a response.

```
@app.route('/least_covid_cases') # defing the things to happen on /least_covid_cases
def get_least_covid_cases():
    # Sorting the data frame as per given criteria in acending and selecting the top most record and state column
    least_covid_cases=df.sort(df.confirm.cast("Long")).select(col("state")).collect()[0][0]
    return jsonify({'get_least_covid_cases':least_covid_cases}) # returning the jsonfied response
```
This function returns the state with the least number of COVID cases. It sorts the dataframe by the number of confirmed cases in ascending order and selects the state with the least number of cases, which is the first record in the sorted dataframe. The state name is then returned as a JSON object with the key 'get_least_covid_cases'.

```
@app.route('/total_cases') # defing the things to happen on /total_cases
def get_total_cases():
    # Suming the data frame as per given criteria and selecting the sum.
    total_cases=df.select(sum(df.confirm).alias("Total cases")).collect()[0][0]
    return jsonify({'Total Cases':total_cases}) # returning the jsonfied response
```
The function get_total_cases() returns the total number of COVID-19 cases in all states of India by summing the values in the confirm column of the DataFrame df. It uses the select method of the DataFrame to compute the sum and assigns it a label "Total cases" using the alias method. Finally, the function returns a JSON-serialized dictionary containing the label and the sum as its value.

```
@app.route('/most_efficient_state') # defing the things to happen on /most_efficient_state
def get_most_efficient_state():
    # Sorting the data frame as per given criteria in descending and selecting the top most record and state column
    most_efficient_state=df.sort((df.cured.cast("Long")/df.confirm.cast("Long")).desc()).select(col("state")).collect()[0][0]
    return jsonify({'most efficient_state':most_efficient_state}) # returning the jsonfied response

```
The get_most_efficient_state() function is defined to handle the GET request on the '/most_efficient_state' endpoint.

This function calculates the efficiency of each state in handling COVID by computing the ratio of the number of people cured to the number of confirmed cases for each state, sorts the DataFrame in descending order based on this ratio, and selects the state with the highest efficiency. Finally, it returns a JSON response with the state with the highest efficiency as the value of the 'most efficient_state' key.

```
@app.route('/least_efficient_state') # defing the things to happen on /least_efficient_state
def get_least_efficient_state():
    # Sorting the data frame as per given criteria in ascending and selecting the top most record and state column
    least_efficient_state=df.sort((df.cured.cast("Long")/df.confirm.cast("Long")).asc()).select(col("state")).collect()[0][0]
    return jsonify({'least efficient_state':least_efficient_state}) # returning the jsonfied response

```

This route defines a function to get the least efficient state in terms of COVID-19 recovery rate. The function sorts the DataFrame by dividing the number of cured cases by the number of confirmed cases, sorts the result in ascending order and selects the state with the lowest recovery rate as the least efficient state. Finally, it returns a JSON object containing the name of the least efficient state.



### Test run - 
#### Cleaned Dataframe
<img width="465" alt="Screenshot 2023-04-06 at 1 00 16 PM" src="https://user-images.githubusercontent.com/123494344/230307116-027a8538-5ffe-4359-af6b-c4ea088987bc.png">

#### @app.route('/') # defing the things to happen on home path
<img width="934" alt="Screenshot 2023-04-06 at 2 11 48 PM" src="https://user-images.githubusercontent.com/123494344/230323537-db336f9b-8ad5-43cb-be51-d8727a16b788.png">

#### @app.route('/getcsvfile') # defing the things to happen on /most_affected_state
<img width="602" alt="Screenshot 2023-04-06 at 2 09 43 PM" src="https://user-images.githubusercontent.com/123494344/230323286-19db81dd-a5e1-4bd4-8f83-cfcdd138594d.png">

#### @app.route('/most_affected_state') # defing the things to happen on /most_affected_state
<img width="586" alt="Screenshot 2023-04-06 at 1 03 09 PM" src="https://user-images.githubusercontent.com/123494344/230307178-deaa9ca0-d7b5-4c2d-abe1-2b40d6a6528c.png">

#### @app.route('/least_affected_state') # defing the things to happen on /least_affected_state
<img width="719" alt="Screenshot 2023-04-06 at 1 03 24 PM" src="https://user-images.githubusercontent.com/123494344/230307186-fc16e345-1ebd-4640-98ad-065efbdce860.png">

#### @app.route('/highest_covid_cases') # defing the things to happen on /highest_covid_cases
<img width="795" alt="Screenshot 2023-04-06 at 1 03 44 PM" src="https://user-images.githubusercontent.com/123494344/230307209-117abb57-3884-43f2-827a-8423cfb9cd01.png">

#### @app.route('/least_covid_cases') # defing the things to happen on /least_covid_cases
<img width="711" alt="Screenshot 2023-04-06 at 1 04 06 PM" src="https://user-images.githubusercontent.com/123494344/230307237-2f6d6799-dd38-4d09-ba5d-aef801d22f35.png">

#### @app.route('/total_cases') # defing the things to happen on /total_cases
<img width="564" alt="Screenshot 2023-04-06 at 1 04 34 PM" src="https://user-images.githubusercontent.com/123494344/230307302-42243a60-a683-47a5-bdd4-20893355af82.png">

#### @app.route('/most_efficient_state') # defing the things to happen on /most_efficient_state
<img width="714" alt="Screenshot 2023-04-06 at 1 05 14 PM" src="https://user-images.githubusercontent.com/123494344/230307322-57778aef-ded4-4ec5-a1ef-fb781f49db27.png">

#### @app.route('/least_efficient_state') # defing the things to happen on /least_efficient_state
<img width="778" alt="Screenshot 2023-04-06 at 1 05 25 PM" src="https://user-images.githubusercontent.com/123494344/230307381-1ce99f7d-1800-4da8-9aa5-6baa4dfbd2ac.png">
