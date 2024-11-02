from pyspark.sql import SparkSession

# Inicializando a sessão Spark
spark = SparkSession.builder.appName("Visualizar Parquet").getOrCreate()

# Lendo o arquivo Parquet
df = spark.read.parquet(r"D:\clientes")
df1 = spark.read.parquet(r"D:\compras")

# Exibindo as primeiras linhas
df.show(truncate=False)
df1.show(truncate=False)