import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia
from pyspark.sql import functions as F

def main():

    spark = inicializa_spark()

    df = retorna_df_bolsa_familia(spark=spark)

    soma_por_mes = df.groupBy('mes_referencia').sum('valor_parcela').orderBy('mes_referencia')

    soma_por_mes = soma_por_mes.withColumn('Total_Pagamentos',
                                           F.format_number(F.col('sum(valor_parcela)'), 2))
    

    soma_por_mes.select('mes_referencia', 'Total_Pagamentos').show(truncate=False)


if __name__ == '__main__':
    main()