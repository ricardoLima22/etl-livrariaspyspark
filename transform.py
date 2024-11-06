import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType



def df_transform(df,df1,df2,df3):

    ## TRANSFORM COMPRAS
    select_compras = (
        df.select(F.col("id"),
                  F.col("cartao_data_expiracao"),
                  F.col("cartao_bandeira"),
                  F.col("data"), 
                  F.col("cd_livro"),
                  F.col("cd_cliente"))
    )
    # group_by = (
    #     select_compras
    #     .groupby("cartao_bandeira").count()
    # )
    
    transform_compras = (
        select_compras
        .withColumn("cartao_bandeira", F.regexp_replace(F.col("cartao_bandeira"), "[^a-zA-Z]", " "))
        .withColumn("cartao_bandeira", F.split(F.col("cartao_bandeira"), "    ").getItem(0))
        .withColumn("cartao_bandeira", F.split(F.col("cartao_bandeira"), "   ").getItem(0))
    )
    # transform_compras.write.format("parquet")\
    #                     .mode("overwrite")\
    #                     .option("compression", "gzip")\
    #                     .save(r"D:\livraria_tabela\table_compras")


    ## TRANSFORM CLIENTE
    transform_name_cliente = (
        df1
        .withColumn("name", F.when(F.col("name").rlike(r"^(Sra\.|Sr\.|Srta\.|Dr)"), F.expr("substring(name, 5, length(name))"))
                                   .otherwise(F.col("name")))
        .withColumn("name", F.regexp_replace("name", r"\.", ""))
        .withColumn("name", F.trim(F.col("name")))
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
    # cliente_transform.write.format("parquet")\
    #                    .mode("overwrite")\
    #                    .option("compression", "gzip")\
    #                    .save(r"D:\livraria_tabela\table_cliente")


    ##TRANSFORM LIVROS

    select_livros = (
        df2.select(F.col("id"),F.col("data_lancamento"), F.col("numero_paginas"),F.col("preco"))
    )
    select_livros.write.format("parquet")\
                       .mode("overwrite")\
                       .option("compression", "gzip")\
                       .save(r"D:\livraria_tabela\table_livro")

    ## TRANSFORM AUTORES
    select_autores = (
        df3.select(F.col("id"), F.col("titulo"),F.col("autor"))
    )
    # select_autores.write.format("parquet")\
    #                    .mode("overwrite")\
    #                    .option("compression", "gzip")\
    #                    .save(r"D:\livraria_tabela\table_autores")


    ## JOINs
    join_cliente_compras = (
        cliente_transform.alias("cl")
        .join(transform_compras.alias("cp"), F.col("cp.cd_cliente") == F.col("cl.id"), how="inner")
    )

    join_livros_autores = (
        select_autores.alias("at")
        .join(select_livros.alias("lv"), F.col("at.id") == F.col("lv.id"), how="inner") 
    )

    selectjoin_livros_autores = (
        join_livros_autores.select(F.col("lv.id"),
                                   F.col("at.titulo"),
                                   F.col("at.autor"),
                                   F.col("lv.data_lancamento"),
                                   F.col("lv.numero_paginas"),
                                   F.col("lv.preco"))
    )
    # selectjoin_livros_autores.write.format("parquet")\
    #                    .mode("overwrite")\
    #                    .option("compression", "gzip")\
    #                    .save(r"D:\livraria_tabela\tabelas_joins\table_livros_autores_joins")


    selectjoin_cliente_compra = (
        join_cliente_compras.select(F.col("cl.id").alias("id_clientes"),  
                                    F.col("cl.name"), 
                                    F.col("cl.idade"),
                                    F.col("cl.data_de_nascimento"),
                                    F.col("cl.estado"),
                                    F.col("cp.id").alias("id_compras"),
                                    F.col("cp.cartao_bandeira"),
                                    F.col("cp.data"),
                                    F.col("cp.cd_livro")
        )
    )
    # selectjoin_cliente_compra.write.format("parquet")\
    #                    .mode("overwrite")\
    #                    .option("compression", "gzip")\
    #                    .save(r"D:\livraria_tabela\tabelas_joins\table_compras_clientes_joins")


    join_geral = (
        selectjoin_cliente_compra.alias("cc")
        .join(selectjoin_livros_autores.alias("la"), F.col("la.id") == F.col("cc.cd_livro"), how="inner" )
    )
    
    select_join = (
        join_geral.select(F.col("cc.id_clientes"),  
                                    F.col("cc.name"), 
                                    F.col("cc.idade"),
                                    F.col("cc.data_de_nascimento"),
                                    F.col("cc.estado"),
                                    F.col("cc.id_compras"),
                                    F.col("cc.cartao_bandeira"),
                                    F.col("cc.data"),
                                    F.col("la.id").alias("id_livro_autor"),
                                    F.col("la.titulo"),
                                    F.col("la.autor"),
                                    F.col("la.data_lancamento"),
                                    F.col("la.numero_paginas"),
                                    F.col("la.preco")
        )
    )
    # select_join.write.format("parquet")\
    #                    .mode("overwrite")\
    #                    .option("compression", "gzip")\
    #                    .save(r"D:\livraria_tabela\tabelas_joins\table_geral_joins")

    return  transform_compras, selectjoin_cliente_compra, selectjoin_livros_autores, select_join

