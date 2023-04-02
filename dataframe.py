import requests
from pyspark.sql.functions import col
from pyspark import SparkContext
from pyspark.sql import SparkSession
from config import KEY
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col
def get_data():
	url = "https://covid-19-india2.p.rapidapi.com/details.php"
	headers = {
		"X-RapidAPI-Key": KEY,
		"X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
	}
	response = requests.request("GET", url, headers=headers)
	return response

def clean_data(df):
    df=df.drop('_corrupt_record')
    df=df.where((df.state.isNotNull()) & (df.state!=''))
    df = df.withColumn("confirm",col("confirm").cast("Long")).withColumn("cured",col("cured").cast("Long")).withColumn("death",col("death").cast("Long"))
    df=df.select("slno","state","confirm","cured","death","total")
    df=df.withColumn('state', regexp_replace('state', '\*',""))
    return df

spark = SparkSession.builder.master('local[*]').getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")
response=get_data()
json_rdd = sc.parallelize(response.json().values())
df = spark.read.json(json_rdd)
df=clean_data(df)