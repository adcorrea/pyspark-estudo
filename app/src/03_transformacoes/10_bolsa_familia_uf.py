from pyspark.sql import SparkSession
from pyspark.sql.functions import count

def main() -> None:

    spark = SparkSession.builder \
                        .appName('Product Analysis') \
                        .config('spark.ui.port', '4041') \
                        .getOrCreate()
    

    path_file = './app/data/bolsafamilia/202306_NovoBolsaFamilia.csv'

    df = spark.read \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .option('enconding', 'ISO-8859-1') \
        .option('sep', ';') \
        .option('quote', "\"") \
        .csv(path_file)

    df_uf = df.groupBy('UF').agg(count('NIS FAVORECIDO').alias('Benefeciarios')).orderBy('Benefeciarios', ascending=False)

    df_uf.show()

if __name__ == '__main__':
    main()