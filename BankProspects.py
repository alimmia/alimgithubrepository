#create a spark session 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("mySparkSession").getOrCreate()
bankProspectsDF = spark.read.csv("/home/alimmia9/bankraw/retailstore.csv",header=True)
bankProspectsDF.show()
#Remove row that has country unknown
#banking project; remove row that has country unknown; replace null value with mean value
bankProspectsDF1 = bankProspectsDF.filter(bankProspectsDF['country'] != "unknow")
bankProspectsDF1.show()
bankProspectsDF1.printSchema()

#change datatype of age to int and salary to float
from pyspark.sql.types import IntegerType,FloatType
bankProspectsDF2 = bankProspectsDF1.withColumn("age",bankProspectsDF1["age"].cast(IntegerType())).withColumn("salary",bankProspectsDF1["salary"].cast(FloatType())) 
bankProspectsDF2.printSchema()

#replace null value of age and salary to mean value
from pyspark.sql.functions import mean
mean_age_val = bankProspectsDF2.select(mean(bankProspectsDF2['age'])).collect()
mean_age_val
mean_age = mean_age_val[0][0]
mean_salary_val = bankProspectsDF2.select(mean(bankProspectsDF2['salary'])).collect()
mean_salary_val
mean_salary = mean_age_val[0][0]
bankProspectsDF3 = bankProspectsDF2.na.fill(mean_age,["age"])
bankProspectsDF4 = bankProspectsDF3.na.fill(mean_salary,["salary"])
bankProspectsDF4.show()

#save in a folder 
bankProspectsDF4.write.format("csv").save("/home/alimmia9/bank_prospect_transformed")
 