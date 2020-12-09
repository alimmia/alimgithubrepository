from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("mySparkSession").getOrCreate()
bankProspectsDF = spark.read.csv("/home/alimmia9/bankraw/retailstore.csv",header=True)
bankProspectsDF.show()