import time

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from decimal import Decimal
from datetime import date, datetime

#sudo cp /usr/lib/hudi/hudi-spark3-bundle_2.12-0.12.2-amzn-0.jar /usr/lib/spark/jars/ 添加hudi依赖包

# 初始化SparkSession (需包含Hudi配置)
spark = SparkSession.builder \
    .appName("HudiTableCreation") \
    .config("spark.jars", "/usr/lib/spark/jars/hudi-spark3-bundle_2.12-0.12.2-amzn-0.jar") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .getOrCreate()

# Create a DataFrame
inputDF = spark.createDataFrame(
    [
        ("100", "2015-01-01", "2015-01-02T13:51:39.340396Z"),
        ("101", "2015-01-01", "2015-01-02T12:14:58.597216Z"),
        ("102", "2015-01-01", "2015-01-02T13:51:40.417052Z"),
        ("103", "2015-01-01", "2015-01-02T13:51:40.519832Z"),
        ("104", "2015-01-02", "2015-01-02T12:15:00.512679Z"),
        ("105", "2015-01-02", "2015-01-02T13:51:42.248818Z"),
    ],
    ["id", "creation_date", "last_update_time"]
)

# Specify common DataSourceWriteOptions in the single hudiOptions variable
hudiOptions = {
'hoodie.table.name': 'hudi_sale_test',
'hoodie.datasource.write.recordkey.field': 'id',
'hoodie.datasource.write.partitionpath.field': 'creation_date',
'hoodie.datasource.write.precombine.field': 'last_update_time',
'hoodie.datasource.hive_sync.enable': 'true',
'hoodie.datasource.hive_sync.database' : 'hudi_test',
'hoodie.datasource.hive_sync.table': 'hudi_sale_test',
'hoodie.datasource.hive_sync.partition_fields': 'creation_date',
'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
'hoodie.cleaner.policy' : 'KEEP_LATEST_COMMITS',
'hoodie.cleaner.commits.retained' : 5,
'hoodie.archive.enabled': 'true',
'hoodie.archive.min.commits': '10',
'hoodie.archive.max.commits': '30',
# Enable the cleaner
'hoodie.clean.automatic': 'true',
'hoodie.clean.async': 'true',

# Set the Hudi index type
'hoodie.index.type': 'BLOOM'
}

# Write a DataFrame as a Hudi dataset
inputDF.write \
.format('org.apache.hudi') \
.option('hoodie.datasource.write.operation', 'insert') \
.options(**hudiOptions) \
.mode('overwrite') \
.save('s3://allan-bigdata-test/hudi_test/hudi_sale_table/')

print("Hudi Table Created!!!!!!")

