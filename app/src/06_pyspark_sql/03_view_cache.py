import config 
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia


def main():

    spark = inicializa_spark()

    bolsa_familia_df = retorna_df_bolsa_familia(spark=spark)

    bolsa_familia_df.createOrReplaceTempView('bolsa_familia')


    spark.sql("""
              SELECT * FROM bolsa_familia WHERE nome_municipio LIKE 'PAUL%'
              """).cache().createOrReplaceTempView('filtered_temp')
    
    spark.sql("""
                    SELECT * FROM filtered_temp
              """).show(truncate=False)
    

if __name__ == '__main__':
    main()