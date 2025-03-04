from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName('spark') \
                                    .config('spark.ui.port','4041') \
                                    .getOrCreate()
    
    sc = spark.sparkContext

    valores_parcelas = sc.parallelize([120.00,150.00,95.00,200.00])

    reajuste_rdd = valores_parcelas.map(lambda x: x * 1.1)

    print(reajuste_rdd.collect())


if __name__ == '__main__':
    main()