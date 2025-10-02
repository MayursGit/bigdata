from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

users_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("current_age", IntegerType(), True),
    StructField("retirement_age", IntegerType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("birth_month", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("per_capita_income", DoubleType(), True),
    StructField("yearly_income", DoubleType(), True),
    StructField("total_debt", DoubleType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("num_credit_cards", IntegerType(), True)
])
