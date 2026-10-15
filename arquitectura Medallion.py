from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Definir el esquema estricto esperado del evento financiero
expense_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("cost_center_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# 2. Leer desde la tabla Delta de la Capa Bronze en modo streaming
bronze_stream_df = spark.readStream \
    .table("bronze_expenses")

# 3. Parsear el JSON crudo aplicando el esquema y separando los campos
silver_parsed_df = bronze_stream_df \
    .withColumn("parsed_json", from_json(col("raw_json"), expense_schema)) \
    .select("parsed_json.*", "ingestion_timestamp")

# 4. Limpieza y validación: Filtrar registros nulos o montos negativos/cero
silver_cleaned_df = silver_parsed_df \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .withColumn("event_timestamp", col("timestamp").cast(TimestampType())) \
    .drop("timestamp")

# 5. Escribir el flujo limpio en la tabla Delta de la Capa Silver
silver_query = silver_cleaned_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/delta/silver_expenses_checkpoint") \
    .table("silver_expenses")

print("✨ Capa Silver activa: Datos parseados, validados y guardados en formato columnar Delta.")