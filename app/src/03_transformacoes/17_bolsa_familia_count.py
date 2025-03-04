import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia


def main ():
    spark = inicializa_spark()

    df = retorna_df_bolsa_familia(spark=spark)

    df.groupBy('nome_municipio').count().show(truncate=False)

    #df.show(truncate=False)


if __name__ == '__main__':
    main()