# importing required libraries
import requests
from pyspark.sql.functions import col
from pyspark import SparkContext
from pyspark.sql import SparkSession
from config import KEY # Importing key from config file(Hiding the key)
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col

# Return : response Object
# function to get the data from given api, and return the response object
def get_data():
	url = "https://covid-19-india2.p.rapidapi.com/details.php"
	headers = {
		"X-RapidAPI-Key": "86633eb492mshdbbbd4c134b0c84p1fd8d6jsnaddc0691b003",
		"X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
	}
	response = requests.request("GET", url, headers=headers)
	return response

# Return : cleaned dataframe
# Fucntion to clean the given dataframe, clenaing involves removing corrupted records, empty, void records,
#  and striping the unwanted parts in staten names

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

spark = SparkSession.builder.master('local[*]').getOrCreate() # Creating a spark session
sc = SparkContext.getOrCreate() # creating a Spark context
sc.setLogLevel("ERROR")
response=get_data() # getting the response objet from the above created method
json_rdd = sc.parallelize(response.json().values()) # creating a rdd from the data fetched above
df = spark.read.json(json_rdd) # creating the dataframe for the above created rdd
df=clean_data(df) # cleaning the above created data frame
