import time
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
stream_df = (spark.readStream.format('socket')
                             .option('host', 'localhost')
                             .option('port', 22223)
                             .load())

json_df = stream_df.selectExpr("CAST(value AS STRING) AS payload")

writer = (
    json_df.writeStream
           .queryName('iss')
           .format('memory')
           .outputMode('append')
)

streamer = writer.start()


for _ in range(5):
    df = spark.sql("""
    SELECT CAST(get_json_object(payload, '$.iss_position.latitude') AS FLOAT) AS latitude,
           CAST(get_json_object(payload, '$.iss_position.longitude') AS FLOAT) AS longitude 
    FROM iss
    """)
    
    df.show(10)
    
    print(df)
    time.sleep(5)
    
streamer.awaitTermination(timeout=10)
print('streaming done!')

fig=px.scatter_geo(df, lat="Latitude", lon="Longitude")
fig.update_layout(title='Latitude and Longitude of World map',title_x=0.5)
fig.show()