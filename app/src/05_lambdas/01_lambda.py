from pyspark.sql import SparkSession, DataFrame
from pyspark import RDD


def main():

    spark = SparkSession.builder.appName('spark') \
                                .config('spark.ui.port','4041') \
                                .getOrCreate()
    
    sc = spark.sparkContext

    numeros_rdd = sc.parallelize([1, 2, 3, 4])

    print(numeros_rdd.take(4))

    triplicado_rdd = numeros_rdd.map(lambda x: x * 3)

    print(triplicado_rdd.take(4))





if __name__ == '__main__':
    main()