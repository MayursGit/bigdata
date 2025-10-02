from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime 

def add_audit_cols(df,filename):
    def _(df):
        df = df.withColumn("batch_id",lit(filename)).withColumn("create_date",lit(current_date())).withColumn("create_user",lit('cpprod')).withColumn("update_date",lit(current_date())).withColumn("update_user",lit(None))
        return df
    return _