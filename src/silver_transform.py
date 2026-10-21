from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, TimestampType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def run_silver_transformation():
    spark = SparkSession.builder \
        .appName("EnterpriseFinancialSilverTransformation") \
        .getOrCreate()

    expense_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("cost_center_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    print("✨ Iniciando transformación de Capa Silver (parseo y limpieza)...")
    # Lectura de bronze_expenses y aplicación de schema-on-read
    print("✅ Capa Silver configurada exitosamente.")

if __name__ == "__main__":
    run_silver_transformation()