from pyspark.sql import SparkSession
import pyspark.sql.functions as F
# Inicializando a sessão Spark
spark = SparkSession.builder.appName("Visualizar Parquet").getOrCreate()

# Lendo o arquivo Parquet
df = spark.read.parquet(r"D:\clientes")
df1 = spark.read.parquet(r"D:\compras")
df2 = spark.read.parquet(r"D:\table_joins")

# Exibindo as primeiras linhas
df.show(truncate=False)
df1.show(truncate=False)
df2.show(truncate=False)

teste = df2.where(F.col("estado").isin("ES"))

teste.write.format("console").save()