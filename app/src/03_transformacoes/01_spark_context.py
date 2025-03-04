from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame





def main() -> None:

    # Inicia ums sessao spark
    spark = SparkSession.builder.appName('ProductDataAnalysis') \
                                .config('spark.ui.port','4041') \
                                .getOrCreate()

    # Atribui o SparkContext à variavel sc
    sc = spark.sparkContext

    # Carrega os dados do arquivo CSV em um RDD
    file_path = './app/data/pricerunner_aggregate.csv'

    # Criar um objeto RDD
    rdd = sc.textFile(file_path)

    # Fazendo um verificação para confirmar se de fato a variável rdd
    # é um objeto do tipo RDD
    if isinstance(rdd, RDD):
        print('Estamos lidando com RDD')
        print(rdd.toDebugString())
    elif isinstance(rdd, DataFrame):
        print('Estamos lidando com um dataframe')
        rdd.printSchema()
        rdd.explain(True)


    # Criar um objeto DataFrame
    # Utilizando a SparkSession (geralmente acessível através da variável 'spark')
    df = spark.read.csv(file_path, header=True, inferSchema=True)


    if isinstance(df, RDD):
        print('Estamos lidando com RDD')
        print(df.toDebugString())
    elif isinstance(df, DataFrame):
        print('Estamos lidando com um dataframe')
        df.printSchema()
        df.explain(True)


if __name__ == '__main__':
    main()
