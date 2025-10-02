from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


cards_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("card_brand", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_number", LongType(), True),
    StructField("expires", StringType(), True),
    StructField("cvv", IntegerType(), True),
    StructField("has_chip", StringType(), True),
    StructField("num_cards_issued", IntegerType(), True),
    StructField("credit_limit", StringType(), True),  # might be better as DoubleType if numeric
    StructField("acct_open_date", StringType(), True),  # could also be DateType if parsed
    StructField("year_pin_last_changed", IntegerType(), True),
    StructField("card_on_dark_web", StringType(), True)
])
