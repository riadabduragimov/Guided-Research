from pyspark.sql import SparkSession
import time



# 2. Start Spark Session (connects to your Spark master & Hive metastore)
spark = (
    SparkSession.builder
    .appName("ListenerTestApp")
    .master("spark://spark-master:7077")  # Change if needed
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)

# 3. Register the listener

# 4. Run a sample query — you can replace this
query = "SELECT * FROM test_db.customer LIMIT 10"
print(f"\nRunning Query: {query}\n")

df = spark.sql(query)
df.show()

# Optional: sleep for a bit to let Spark finish logging all output
time.sleep(5)

# Stop session if running outside of a notebook
spark.stop()
