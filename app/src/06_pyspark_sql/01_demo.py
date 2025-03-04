import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia

def main():
    spark = inicializa_spark()

    bolsa_familia_df = retorna_df_bolsa_familia(spark=spark)


    bolsa_familia_df.createOrReplaceTempView('bolsa_familia')


    spark.sql('SELECT * FROM bolsa_familia').show()


if __name__ == '__main__':
    main()