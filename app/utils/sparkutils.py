from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F



def inicializa_spark() -> SparkSession:
    spark = SparkSession.builder \
                        .appName('Spark Utils') \
                        .config('spark.ui.port','4041') \
                        .getOrCreate()
    
    return spark

def retorna_df_bolsa_familia(spark) -> DataFrame:
    if not isinstance(spark, SparkSession):
        raise TypeError('instancia de Spark inválida!')
    
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

    # total_registros_antes = cleared_df.count()

    cleared_df = cleared_df.dropDuplicates()

    # total_registros_depois = cleared_df.count()

    # print(f'Total de registros antes: {total_registros_antes}')
    # print(f'Total de registros depois: {total_registros_depois}')
    # print(f'Total de registros duplicados: {total_registros_antes - total_registros_depois}')

    cleared_df = cleared_df.withColumn('ano_referencia', F.year(F.to_date(cleared_df['mes_referencia'],'yyyyMM')))
    cleared_df = cleared_df.withColumn('mes_referencia', F.month(F.to_date(cleared_df['mes_referencia'],'yyyyMM')))

    
    return cleared_df