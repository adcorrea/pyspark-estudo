import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia


def main():
    spark = inicializa_spark()

    df_bolsa_familia = retorna_df_bolsa_familia(spark=spark)

    df_bolsa_familia.filter(df_bolsa_familia['UF'] == 'BA') \
                        .groupBy('nome_municipio').avg('valor_parcela') \
                        .show(truncate=False)


if __name__ == '__main__':
    main()