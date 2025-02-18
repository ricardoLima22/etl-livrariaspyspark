from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StringType,IntegerType,DateType,StructField
from silver import df_transform    
import sys

def main():
    spark = SparkSession.builder.appName("Silver").getOrCreate()

    #extract

    compras = spark.read.parquet(r".\docs\COMPRAS.parquet", header = True, inferSchema = True, sep = "|").alias("cp")
    clientes = spark.read.parquet(r".\docs\CLIENTES.parquet" ,header = True, inferSchema = True, sep = "|").alias("cl")
    livros = spark.read.parquet(r".\docs\LIVROS.parquet" ,header = True, inferSchema = True, sep = "|").alias("lv")
    autores = spark.read.parquet(r".\docs\AUTORES.parquet", header = True, inferSchema = True, sep = "|").alias("at")

    result_compras, result_clientes, result_livros, autores = df_transform(df=compras,df1=clientes,df2=livros,df3=autores)

    result_compras.show(20,False)
    result_clientes.show(20,False)
    result_livros.show(20,False)

    autores.show(20,False)



    # result_autores.write.format("parquet")\
    #                     .mode("overwrite")\
    #                     .option("compression", "gzip")\
    #                     .save(r"D:\livraria_tabela\table_join")
    
    spark.stop()
if __name__ == "__main__":
    main()
    