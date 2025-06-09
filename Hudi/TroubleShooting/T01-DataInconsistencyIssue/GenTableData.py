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

# 定义表结构 (示例)
product_sales_schema = StructType([
    # 主键字段
    StructField("product_id", StringType(), nullable=False),  # 作为recordkey

    # 时间字段
    StructField("last_sales_time", IntegerType()),  # 作为precombine字段
    StructField("sales_date", DateType()),  # 作为分区键

    # 基础销售信息
    StructField("sku_code", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("store_id", StringType()),
    StructField("sales_region", StringType()),

    # 商品信息
    StructField("product_name", StringType()),
    StructField("category_id", IntegerType()),
    StructField("brand_id", IntegerType()),

    # 销售指标
    StructField("unit_price", DecimalType(10, 2)),
    StructField("quantity", IntegerType()),
    StructField("total_amount", DecimalType(12, 2)),

    # 其他字段... (实际应有50个字段，此处简化为示例)
    StructField("payment_method", StringType()),
    StructField("shipping_method", StringType()),
    StructField("order_status", StringType()),
    StructField("member_level", StringType()),
    StructField("source_system", StringType()),

    # 状态字段
    StructField("is_active", BooleanType()),
    StructField("version", IntegerType())
])

# 创建示例DataFrame (实际使用时可从源数据读取)
data = [(
        "P1001",  # product_id (主键)
        int(datetime.now().timestamp()),  # last_sales_time
        date(2023, 10, 15),  # sales_date (分区键)
        "SKU-001", "ORD202310150001", "CUST10001", "STORE101", "East",
        "智能手机X", 101, 201,
        Decimal("1617.00"), 1, Decimal("1607.00"),
        "CreditCard", "Express", "Completed", "Gold", "ERP",
        True, 1
    ),(
        "P1002",  # product_id (主键)
        int(datetime.now().timestamp()),  # last_sales_time
        date(2023, 10, 15),  # sales_date (分区键)
        "SKU-001", "ORD202310150001", "CUST10001", "STORE101", "East",
        "智能手机X", 101, 201,
        Decimal("1618.00"), 1, Decimal("1608.00"),
        "CreditCard", "Express", "Completed", "Gold", "ERP",
        True, 1
    ),(
        "P1003",  # product_id (主键)
        int(datetime.now().timestamp()),  # last_sales_time
        date(2023, 10, 15),  # sales_date (分区键)
        "SKU-001", "ORD202310150001", "CUST10001", "STORE101", "East",
        "智能手机X", 101, 201,
        Decimal("1619.00"), 1, Decimal("1609.00"),
        "CreditCard", "Express", "Completed", "Gold", "ERP",
        True, 1
    )]

df = spark.createDataFrame(data, product_sales_schema)

# 目标存储路径 (需替换为实际路径)
target_path = "s3://allan-bigdata-test/hudi_test/hudi_sale_table/"
tableName = "hudi_sale_table"

# 写入Hudi表(首次写入用overwrite，后续写入数据用append)
df.write.format("hudi").option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .option("hoodie.datasource.write.recordkey.field", "product_id") \
    .option("hoodie.datasource.write.precombine.field", "last_sales_time") \
    .option("hoodie.datasource.write.partitionpath.field", "sales_date") \
    .option("hoodie.table.name", tableName) \
    .option("hoodie.datasource.hive_sync.enable", "true") \
    .option("hoodie.datasource.hive_sync.database", "hudi_test") \
    .option("hoodie.datasource.hive_sync.table", tableName) \
    .option("hoodie.datasource.hive_sync.mode", "hms") \
    .option("hoodie.datasource.hive_sync.use_jdbc", "false") \
    .option("hoodie.datasource.write.hive_style_partitioning", "true") \
    .option("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS") \
    .option("hoodie.cleaner.commits.retained", 5) \
    .option("hoodie.keep.min.commits", 6) \
    .option("hoodie.keep.max.commits", 10) \
    .mode("Append") \
    .save(target_path)

print("Hudi Table Created!!!!!!")

