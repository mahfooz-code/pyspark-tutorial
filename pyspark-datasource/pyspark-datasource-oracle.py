from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("PySpark App") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@host:1527/dbname") \
    .option("dbtable", "dual") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

df.printSchema()
