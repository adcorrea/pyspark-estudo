from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName('Product Analysis') \
                                .config('spark.ui.port','4041') \
                                .getOrCreate()

    sc = spark.sparkContext

    path_file = './app/data/pricerunner_aggregate.csv'

    rdd = sc.textFile(path_file)

    # definindo a fração de amostragem somente para reprodutibilidade
    fraction = 0.1 # 10% dos dados
    seed = 42 # somente para garantir que a amostragem seja a mesma em execuções diferentes

    # criando a amostra aleatória
    sempled_rdd = rdd.sample(False, fraction, seed)

    # coletando e imprimindo os resultados
    sampled_data = sempled_rdd.collect()

    print(sampled_data)

if __name__ == '__main__':
    main()

