import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia
from pyspark.sql.functions import col,regexp_replace


def main():
    spark = inicializa_spark()

    bolsa_familia_df = retorna_df_bolsa_familia(spark=spark)


    bolsa_familia_df = bolsa_familia_df.withColumn('valor_str', col('valor_parcela').cast('string'))

    bolsa_familia_df = bolsa_familia_df.withColumn('valor_parcela_float', regexp_replace('valor_str', ',', '.').cast('float'))

    bolsa_familia_df = bolsa_familia_df.drop('valor_str')

    valore_parcela_rdd = bolsa_familia_df.select('valor_parcela_float').rdd.flatMap(lambda row: row)

    arredondados_rdd = valore_parcela_rdd.map(lambda valor: round(valor))

    print(arredondados_rdd.take(10))


if __name__ == '__main__':
    main()