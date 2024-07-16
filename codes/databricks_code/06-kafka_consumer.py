# Databricks notebook source
from pyspark.sql.functions import col,from_json

from pyspark.sql.types import *

# COMMAND ----------

bootstrap_server = "pkc-56d1g.eastus.azure.confluent.cloud:9092" 

jaas_module= "org.apache.kafka.common.security.plain.PlainLoginModule" 

cluster_api_key= "ZF2Q6AFNI4D523WD"

cluster_api_secret= "CpHTEEkE1W7F3kl/Q2w67TOkterNjVXDJUNZymx++bOBpiWZMatK1vw3eBI9OioI"

# COMMAND ----------

kafka_df = (
    spark.readStream
         .format("kafka")
         .option('kafka.bootstrap.servers', bootstrap_server)
         .option("kafka.security.protocol", "SASL_SSL")
         .option("kafka.sasl.mechanism", "PLAIN")
         .option("kafka.sasl.jaas.config", f"{jaas_module} required username='{cluster_api_key}' password='{cluster_api_secret}';")
         .option("subscribe", "invoice_1")
         .option("maxOffsetsPerTrigger", 10)
         .load()
)


# COMMAND ----------

df=(kafka_df.withColumn("key",col("key").cast("string"))
        .withColumn("value",col("value").cast("string"))
)

# COMMAND ----------

schema_json= StructType([
                         StructField("InvoiceNumber",StringType(),True),
                         StructField("CreatedTime",IntegerType(),True),
                         StructField("StoreID",StringType(),True),
                         StructField("PosID",StringType(),True),
                         StructField("CashierID",StringType(),True),
                         StructField("CustomerType",StringType(),True),
                         StructField("CustomerCardNo",StringType(),True),
                         StructField("TotalAmount",DoubleType(),True),
                         StructField("NumberOfItems",LongType(),True),
                         StructField("PaymentMethod",LongType(),True),
                         StructField("TaxableAmount",DoubleType(),True),
                         StructField("CGST",DoubleType(),True),
                         StructField("SGST",DoubleType(),True),
                         StructField("CESS",DoubleType(),True),
                         StructField("DeliveryType",StringType(),True),
                         StructField("DeliveryAddress",
                                                      StructType([
                                                                    StructField("AddressLine",StringType(),True),
                                                                    StructField("City",StringType(),True),
                                                                    StructField("ContactNumber",StringType(),True),
                                                                    StructField("PinCode",StringType(),True),
                                                                    StructField("State",StringType(),True),
                                                                
                                                      ]),
                                                         True
                                    ),
                         StructField("InvoiceLineItems",ArrayType(
                                                       StructType([
                                                                        StructField("ItemCode", StringType(), True),
                                                                        StructField("ItemDescription", StringType(), True),
                                                                        StructField("ItemPrice", DoubleType(), True),
                                                                        StructField("ItemQty", LongType(), True),
                                                                        StructField("TotalValue", DoubleType(), True)
                                                                    ]
                                                                  )
                                                       ),True
                                     
                                     )
                         
            ]

            )
    

# COMMAND ----------

modified_df=(df.withColumn('exploded_json',
               from_json(col('value'),
                         schema_json
                        )
               )
    .drop(col('value'))
)



# COMMAND ----------

stream_q=(modified_df.writeStream
           .queryName("bronze-ingestion-kafka-stream") 
           .outputMode("append")
           .option("checkpointLocation","/FileStore/data/checkpoint_3/bronze-ingestion-kafka-stream")
)

# COMMAND ----------

(stream_q.format('delta')
        .toTable('kafka_ingestion_stream_3')
)

# COMMAND ----------



# COMMAND ----------


