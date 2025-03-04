import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia
from pyspark.sql.functions import col


def main():
    spark = inicializa_spark()

    bolsa_familia_df = retorna_df_bolsa_familia(spark=spark)

    pagamentos_2023_df = bolsa_familia_df.filter(col('mes_competencia').cast('string').substr(1, 4) == '2023')

    pagamentos_2023_df.show(truncate=False)



if __name__ == '__main__':
    main()