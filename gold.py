from pyspark.sql import SparkSession
import pyspark.sql.functions as F
# Inicializando a sessão Spark
spark = SparkSession.builder.appName("Visualizar Parquet")\
                            .getOrCreate()

# Lendo o arquivo Parquet

dflivros = spark.read\
                    .format("parquet")\
                    .option("compression", "gzip")\
                    .option("header", True)\
                    .option("inferSchema",True)\
                    .load(r"D:\livraria_tabela\table_livros")
# dflivros.show(5)

dfautores = spark.read\
                      .format("parquet")\
                      .option("compression", "gzip")\
                      .option("header", True)\
                      .option("inferSchema", True)\
                      .load(r"D:\livraria_tabela\table_autores")
# dfautores.show(5)

dfcompras = spark.read\
                      .format("parquet")\
                      .option("compression","gzip")\
                      .option("header", True)\
                      .option("inferSchema", True)\
                      .load(r"D:\livraria_tabela\table_compras")
# dfcompras.show(5)

dfclientes = spark.read\
                       .format("parquet")\
                       .option("compression", "gzip")\
                       .option("header", True)\
                       .option("inferSchema", True)\
                       .load(r"D:\livraria_tabela\table_clientes")


dfjoin = spark.read\
                    .format("parquet")\
                    .option("compression", "gzip")\
                    .option("header", True)\
                    .option("inferSchema", True)\
                    .load(r"D:\livraria_tabela\tabelas_joins\table_geral_joins")

dfjoin1 =dfjoin.select(F.col("estado"), F.col("preco"), F.col("data"))

dfjoin1.createOrReplaceTempView("teste_ts")

spark.sql(
    '''
    WITH table as (
        SELECT
        estado, 
        preco,
        year(data) AS data
    FROM teste_ts
    )
    SELECT estado,
          ROUND(SUM(preco),1) AS preco_total
         ,data
    FROM table
    WHERE estado == 'ES'
    GROUP BY estado,data
    ORDER BY preco_total desc

    '''
).show()

# acentos     = "áàãâéèêíìóòõôúùûüç"
# sem_acentos = "aaaaeeeiioooouuuuc"

# df_no_accent = dfjoin.withColumn(
#     "texto_sem_acento", 
#     F.translate("titulo", acentos, sem_acentos)
# )
# df_no_accent.show()


# df_cleaned = dfjoin.withColumn(
#     "teste", 
#     F.regexp_replace(F.col("titulo"), "[^a-zA-Z0-9 ]", "")
# )


# df_no_accent.write.format("console").save()

# teste = (
#     dfjoin.where((F.col("estado") == 'ES') & (F.col("idade").between(20,30))).where(F.col("name") == "pedro henrique almeida")
#     .withColumn("autor", F.translate(F.col("autor"), acentos, sem_acentos))

# )
# teste.show()

# dfjoin.printSchema()

# print(dfjoin.schema)

# print(dfjoin.columns)
# dfpartitiones = spark.read\
#                 .format("parquet")\
#                 .option("compression","gzip")\
#                 .option("header",True)\
#                 .option("inferSchema",True)\
#                 .load(r"D:\table_joins\partitionEstado_parquet_zip\estado=ES")



# contagem_nulos = dfjoin.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in dfjoin.columns])
# contagem_nulos.write.format("console").save()

# for Loop in dfjoin.columns:
# 	print(Loop)


#SQL 
# dfjoin.createOrReplaceTempView("tabela_teste")

# teste = spark.sql(
#     '''
#     WITH teste as(
#     SELECT 
#         name,
#         preco,
#         year(data) as data
#     FROM tabela_teste
#     GROUP BY name,preco,year(data)
#     )
#     SELECT     
#         name,
#         data,
#     ROUND(sum(preco),2) AS preco_total
#     FROM teste
#     GROUP BY name,data
#     ORDER BY preco_total desc
#     '''
#     )

# teste.groupBy(F.col("data")).count().show()


# teste.createOrReplaceTempView("tabela_datamax")

# spark.sql(
#     '''
#     SELECT *
#     FROM tabela_datamax
#     WHERE data = '2023'
#     ORDER BY preco_total desc
#     LIMIT 3
#     '''
# ).show()


# spark.sql(
#     '''
#     WITH tabela as (
#     SELECT *
# 	FROM tabela_datamax as tbd
# 	WHERE preco_total in (
#             SELECT MAX(preco_total)
# 			FROM tabela_datamax as tda
# 			WHERE tbd.data = tda.data
# 	)ORDER BY preco_total DESC
#     )
#     SELECT
#     ROW_NUMBER()OVER(ORDER BY preco_total DESC) as rank,
#     *
#     FROM tabela
# 	'''
# ).show()