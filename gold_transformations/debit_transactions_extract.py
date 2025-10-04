from pyspark.sql.functions import *
from pyspark.sql.types import *

def debit_transactions_extract(trans_df,cards_df,users_df):
    return(
        trans_df.alias('trans').join(
            cards_df.alias('cards'),
            on=(
            (col('trans.card_id')==col('cards.id')) & 
            (col('cards.card_type')=='Debit')
            ),
            how='inner')\
                .join(
                    users_df.alias('users'),
                    on=(col('trans.client_id')==col('users.id')),
                    how='leftouter'
                )\
            .select(
                col('trans.client_id').alias('UserId'),
                col('users.name').alias('UserName'),
                col('trans.id').alias('TransId'),
                col('trans.date').alias('TransDate'),
                col('trans.card_id').alias('CardId'),
                col('cards.card_brand').alias('CardBrand'),
                col('cards.card_type').alias('CardType'),
                col('cards.card_number').alias('CardNumber'),
                col('trans.amount').alias('TransAmount'),
                col('trans.use_chip').alias('TransType'),
                col('trans.merchant_id').alias('MerchantId'),
                col('trans.merchant_city').alias('MerchantCity'),
                col('trans.merchant_state').alias('MerchantState'),
                col('trans.zip').alias('ZipCode'),
                col('trans.mcc').alias('MCC'),
                col('trans.errors').alias('TransErrors'),
                col('trans.batch_id').alias('BatchId'),
                col('trans.create_date').alias('CreateDate'),
                col('trans.create_user').alias('CreateUser'),
                lit("DebitTransactionsExtract").alias('CreateProcess'),
                col('trans.update_date').alias('UpdateDate'),
                col('trans.update_user').alias('UpdateUser'),
                lit("").alias('UpdateProcess')
            )
    )
# dfs['cards'].count()