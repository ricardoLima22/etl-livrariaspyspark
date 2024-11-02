from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StringType,IntegerType,DateType,StructField
from transform import df_transform    
import sys

def main():
    spark = SparkSession.builder.appName("teste").getOrCreate()

    #extract

    compras = spark.read.parquet(r".\docs\COMPRAS.parquet", header = True, inferSchema = True, sep = "|").alias("cp")
    clientes = spark.read.parquet(r".\docs\CLIENTES.parquet" ,header = True, inferSchema = True, sep = "|").alias("cl")
    livros = spark.read.parquet(r".\docs\LIVROS.parquet" ,header = True, inferSchema = True, sep = "|").alias("lv")


    result_compras, result_clientes = df_transform(df=compras,df1=clientes)

    result_compras.show(20,False)
    result_clientes.show(20,False)

    # result_compras.write.format("parquet").mode("overwrite").save("D:\compras")
    # result_clientes.write.format("parquet").mode("overwrite").save("D:\clientes")

    
    spark.stop()
if __name__ == "__main__":
    main()
    