import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType



def df_transform(df,df1):

    select_compras = (
        df.select(F.col("id"),
                  F.col("cartao_data_expiracao"),
                  F.col("cartao_bandeira"),
                  F.col("data"), 
                  F.col("cd_livro"),
                  F.col("cd_cliente"))
    )

    transform_name_cliente = (
        df1
        .withColumn("name", F.when(F.col("name").rlike(r"^(Sra\.|Sr\.|Srta\.|Dr)"), F.expr("substring(name, 6, length(name))"))
                                   .otherwise(F.col("name")))
    )

    name_cliente = (
        transform_name_cliente
        .withColumn("name", F.trim(F.col("name")))
    )

    create_table_idade = (
        name_cliente
        .withColumn("idade", F.datediff(F.current_date(),F.col("data_de_nascimento"))/365)
    )

    idade = (
        create_table_idade
        .withColumn("idade", F.col("idade").cast("int"))
    )

    cliente_transform = (
        idade.select(F.col("id"),
                   F.col("cpf"),
                   F.col("name"),
                   F.col("idade"),
                   F.col("data_de_nascimento"),
                   F.col("estado"))
    )

    join_cliente_compras = (
        cliente_transform.alias("cl")
        .join(select_compras, select_compras.cd_cliente == cliente_transform.id, how= "inner")
    )
    teste = (
        join_cliente_compras.select(F.col("cp.id"))
    )
    return  select_compras, teste

