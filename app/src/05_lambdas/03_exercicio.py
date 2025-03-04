from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
                            .appName('spark') \
                            .config('spark.ui.port','4041') \
                            .getOrCreate()
    
    sc = spark.sparkContext

    nomes_rdd = sc.parallelize(['Antonio','Donizete', 'Correa', 'Junior'])

    iniciais_rdd = nomes_rdd.map(lambda x: x[0])

    print(iniciais_rdd.collect())


if __name__ == '__main__':
    main()