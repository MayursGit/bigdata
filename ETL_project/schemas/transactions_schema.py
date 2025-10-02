from pyspark.sql.types import StructType, StructField, StringType, IntegerType

transactions_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("date", StringType(), True),  # could be DateType or TimestampType if parsed
    StructField("client_id", IntegerType(), True),
    StructField("card_id", IntegerType(), True),
    StructField("amount", StringType(), True),  # maybe DoubleType if numeric
    StructField("use_chip", StringType(), True),
    StructField("merchant_id", IntegerType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("zip", IntegerType(), True),
    StructField("mcc", IntegerType(), True),
    StructField("errors", StringType(), True)
])
