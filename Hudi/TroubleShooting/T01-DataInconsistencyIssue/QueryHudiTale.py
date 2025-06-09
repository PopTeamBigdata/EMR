import time

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from decimal import Decimal
from datetime import date, datetime

#sudo cp /usr/lib/hudi/hudi-spark3-bundle_2.12-0.12.2-amzn-0.jar /usr/lib/spark/jars/ 添加hudi依赖包

#Spark-SQL CLI 启动Hudi支持
#spark-sql --jars /usr/lib/spark/jars/hudi-spark3-bundle_2.12-0.12.2-amzn-0.jar --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.serializer=org.apache.spark.serializer.KryoSerializer

# 初始化SparkSession (需包含Hudi配置)
spark = SparkSession.builder \
    .appName("HudiTableCreation") \
    .config("spark.jars", "/usr/lib/spark/jars/hudi-spark3-bundle_2.12-0.12.2-amzn-0.jar") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .getOrCreate()

# commit时间戳
target_instant_time = "20250606180704765"

hudi_table_path = "s3://allan-bigdata-test/hudi_test/hudi_sale_table/"

# Read
historical_df = spark.read \
    .format("hudi") \
    .option("as.of.instant", target_instant_time) \
    .load(hudi_table_path)

print("以下是历史数据!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
historical_df.show()

