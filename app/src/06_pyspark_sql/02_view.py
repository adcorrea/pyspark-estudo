import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia


def main():
    spark = inicializa_spark()

    bolsa_familia_df = retorna_df_bolsa_familia(spark=spark)

    bolsa_familia_df.show(truncate=False)

    bolsa_familia_df.createOrReplaceTempView('bolsa_familia') 

    sum_df = spark.sql("""
                SELECT 'nome_municipio', SUM('valor_parcela') AS valor_total
              FROM bolsa_familia
              GROUP BY 'nome_municipio'              
              """)

    sum_df.show(truncate=False)  


if __name__ == '__main__':
    main()