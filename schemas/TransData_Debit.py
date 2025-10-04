from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DateType

schema = StructType([
    StructField("UserId", IntegerType(), True),
    StructField("UserName", StringType(), True),
    StructField("TransId", IntegerType(), True),
    StructField("TransDate", StringType(), True),
    StructField("CardId", IntegerType(), True),
    StructField("CardBrand", StringType(), True),
    StructField("CardType", StringType(), True),
    StructField("CardNumber", LongType(), True),
    StructField("TransAmount", StringType(), True),
    StructField("TransType", StringType(), True),
    StructField("MerchantId", IntegerType(), True),
    StructField("MerchantCity", StringType(), True),
    StructField("MerchantState", StringType(), True),
    StructField("ZipCode", IntegerType(), True),
    StructField("MCC", IntegerType(), True),
    StructField("TransErrors", StringType(), True),
    StructField("BatchId", StringType(), True),
    StructField("CreateDate", DateType(), True),
    StructField("CreateUser", StringType(), True),
    StructField("UpdateDate", DateType(), True),
    StructField("UpdateUser", StringType(), True)
])
