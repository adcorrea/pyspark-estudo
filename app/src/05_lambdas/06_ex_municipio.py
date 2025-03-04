import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia
from pyspark.sql.functions import col, lower

def main():

    spark = inicializa_spark()
    bolsa_familia_df = retorna_df_bolsa_familia(spark=spark)

    #bolsa_familia_df.show(truncate=False)

    paulinia_df = bolsa_familia_df.filter(lower(col('nome_municipio').cast('string')) == 'paulinia')

    paulinia_df.show(truncate=False)


if __name__ == '__main__':
    main()