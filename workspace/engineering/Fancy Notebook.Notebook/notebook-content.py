# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0d3a6f5d-650b-4299-9dc7-0dedded14dd9",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "c981cb11-db75-4141-b3a8-e8dd2d72e73a",
# META       "known_lakehouses": [
# META         {
# META           "id": "0d3a6f5d-650b-4299-9dc7-0dedded14dd9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

api_key = '59f72143983cfee271ddddd4015eeb89'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from datetime import date

# Get today's date
today = date.today().isoformat()

url = 'http://api.marketstack.com/v1/eod'

params = {
    'access_key': api_key,
    'symbols': 'AAPL',
    'sort': 'DESC',
    'date_from': today,
    'date_to': today,
    'limit': 1
}

# Make the request
response = requests.get(url, params=params)

# Process the response
data = response.json().get("data", []) 
data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("adj_high", DoubleType(), True),
    StructField("adj_low", DoubleType(), True),
    StructField("adj_close", DoubleType(), True),
    StructField("adj_open", DoubleType(), True),
    StructField("adj_volume", DoubleType(), True),
    StructField("split_factor", DoubleType(), True),
    StructField("dividend", DoubleType(), True),
    StructField("symbol", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("date", StringType(), True)  # Or TimestampType() if you parse it
])

# Create Spark DataFrame using schema
df = spark.createDataFrame(data, schema=schema)

# Write to Delta table
df.write.format("delta").mode("append").saveAsTable("StockData.AAPL")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
