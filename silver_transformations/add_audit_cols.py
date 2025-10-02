from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime 

#this is for testing commit in git

def add_audit_cols(df,filename):
    def _(df):
        df = df.withColumn("batch_id",lit(filename)).withColumn("create_date",lit(current_date())).withColumn("create_user",lit('cpprod')).withColumn("update_date",lit(current_date())).withColumn("update_user",lit(None).cast('string'))
        return df
    return _