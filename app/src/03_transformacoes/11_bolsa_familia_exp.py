from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import FloatType 

def main():
    spark = SparkSession.builder.appName('Bolsa Familia') \
                                .config('spark.ui.port','4041') \
                                .getOrCreate()
    
    path_file = './app/data/bolsafamilia/202306_NovoBolsaFamilia.csv'

    df = spark.read \
            .option('header', 'true') \
            .option('inferSchema', 'true') \
            .option('enconding', 'ISO-8859-1') \
            .option('sep', ';') \
            .option('quote', "\"") \
            .csv(path_file)
    
    df_renamed = df.select(
                    col('M�S COMPET�NCIA').alias('mes_competencia'),
                    col('M�S REFER�NCIA').alias('mes_referencia'),
                    col('UF'),
                    col('C�DIGO MUNIC�PIO SIAFI').alias('codigo_municiop_siafi'),
                    col('NOME MUNIC�PIO').alias('nome_municipio'),
                    col('CPF FAVORECIDO').alias('cpf_favorecido'),
                    col('NIS FAVORECIDO').alias('nis_favorecido'),
                    col('NOME FAVORECIDO').alias('nome_favorecido'),
                    regexp_replace(col('VALOR PARCELA'), ',', '.').alias('valor_parcela').cast(FloatType())
                    )
    
    cleared_df = df_renamed.fillna({'cpf_favorecido': 'Não informado'})

    cleared_df.show()

    print(f'Total de registros antes da filtragaem: {cleared_df.count()}')

    cleared_df = cleared_df.filter(cleared_df['valor_parcela'] > 0)

    print(f'Total de registros depois da filtragaem: {cleared_df.count()}')

if __name__ == '__main__':
    main()
