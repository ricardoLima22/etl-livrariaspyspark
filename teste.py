from pyspark.sql import SparkSession
import pyspark.sql.functions as F
# Inicializando a sessão Spark
spark = SparkSession.builder.appName("Visualizar Parquet").getOrCreate()

# Lendo o arquivo Parquet

dflivros = spark.read\
                    .format("parquet")\
                    .option("compression", "gzip")\
                    .option("header", True)\
                    .option("inferSchema",True)\
                    .load(r"D:\livraria_tabela\table_livros")
dflivros.show(5)

dfautores = spark.read\
                      .format("parquet")\
                      .option("compression", "gzip")\
                      .option("header", True)\
                      .option("inferSchema", True)\
                      .load(r"D:\livraria_tabela\table_autores")
dfautores.show(5)

dfcompras = spark.read\
                      .format("parquet")\
                      .option("compression","gzip")\
                      .option("header", True)\
                      .option("inferSchema", True)\
                      .load(r"D:\livraria_tabela\table_compras")
dfcompras.show(5)

dfclientes = spark.read\
                       .format("parquet")\
                       .option("compression", "gzip")\
                       .option("header", True)\
                       .option("inferSchema", True)\
                       .load(r"D:\livraria_tabela\table_clientes")
dfclientes.show(5)


dfjoin = spark.read\
                    .format("parquet")\
                    .option("compression", "gzip")\
                    .option("header", True)\
                    .option("inferSchema", True)\
                    .load(r"D:\livraria_tabela\tabelas_joins\table_geral_joins")
dfjoin.write.format("console").save()

# dfpartitiones = spark.read\
#                 .format("parquet")\
#                 .option("compression","gzip")\
#                 .option("header",True)\
#                 .option("inferSchema",True)\
#                 .load(r"D:\table_joins\partitionEstado_parquet_zip\estado=ES")


# df2.createOrReplaceTempView("tabela_teste")

# spark.sql(
#     "SELECT * FROM tabela_teste"
# ).show()
#teste1 = teste.distinct().orderBy(F.desc(F.col("titulo")))