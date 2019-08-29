
from datetime import datetime, date, time

from pyspark import SparkConf ,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

c=SparkConf().setMaster("mymacbook").setAppName("App sdf")
sc = SparkContext(conf=c)
sqlContext = SQLContext(sc)



import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from datetime import datetime, date, time
def formatData(x):
    col0 = int(x[0])
    col1 = x[1]
    col2 = x[2]
    col3 = int(x[3])
    col4 = datetime.strptime(x[4] ,"%m/%d/%y %H:%M")
    col5 = x[5]
    col6 = float(x[6])
    col7 = x[7]
    col8 = int(x[8])
    col9 = x[9]


    return [col0 ,col1 ,col2 ,col3 ,col4 ,col5 ,col6 ,col7 ,col8 ,col9]




New_RDD = sc.textFile("weather-agg-DFE.csv").map(lambda element: element.split(","))
# ,minPartitions=4).map(lambda element: element.split(","))
header = New_RDD.first()
rows = New_RDD.filter(lambda l: l!= header)
# rows.take(10)
# print(rows.count())
rows_RDD = rows.filter(lambda l: len(l) == 10)
rows_RDD.collect()
print(rows_RDD.count())

formattedDatarows = rows_RDD.map(formatData)
# formattedDatarows.take(5)
print(type(formattedDatarows))

schema = StructType([StructField("_unit_id", LongType(), False), StructField("_canary", StringType(), True),
                     StructField("_unit_state", StringType(), True),
                     StructField("_trusted_judgments", IntegerType(), True),
                     StructField("_last_judgment_at", DateType(), True),
                     StructField("author_emotion", StringType(), True), StructField("confidence", FloatType(), True),
                     StructField("gold_answer", StringType(), True), StructField("tweet_id", LongType(), True),
                     StructField("tweet_text", StringType(), True)])

#x = np.arange(rows_RDD.count)

# schema = StructType([StructField("_unit_id", LongType(), False), StructField("_canary", StringType(), True) , StructField("_unit_state", StringType(), True),StructField("_trusted_judgments", IntegerType(), True),StructField("_last_judgment_at", DateType(), True) , StructField("author_emotion", StringType(), True),StructField("confidence", DecimalType(5,2), True),StructField("gold_answer", StringType(), True) , StructField("tweet_id", LongType(), True),StructField("tweet_text", StringType(), True)])
jdbcDF = sqlContext.createDataFrame(formattedDatarows, schema)
# jdbcDF.show()
print(type(jdbcDF))
pdf = jdbcDF.toPandas()
list1 = pdf['confidence']
#print(pdf['_unit_id'])
pdf.plot(kind='scatter',x='_unit_id',y='confidence',color='red')
plt.show()
jdbcDF.write.format('jdbc').options(
    url='jdbc:mysql://localhost/WEATHERDB',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='Weather_Table1',
    user='root',
    password='root').mode('append').save()
