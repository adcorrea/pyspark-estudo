import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia

def main():
    spark = inicializa_spark()

    sc = spark.sparkContext

    bolsa_familia_df = retorna_df_bolsa_familia(spark=spark)

    nome_beneficiarios_rdd = bolsa_familia_df.select('nome_favorecido').rdd.flatMap(lambda row: row)

    iniciais_rdd = nome_beneficiarios_rdd.map(lambda nome: ''.join([x[0].upper() for x in nome.split()]))

    print(iniciais_rdd.take(10))

if __name__ == '__main__':
    main()