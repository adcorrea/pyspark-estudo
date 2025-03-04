from pyspark.sql import SparkSession

# função para adicionar 1 para cada elemento (linha) na partição
def seqOp(acc, value): 

    # cada linha é um registro, então adicionamos 1
    return acc + 1
    

# Função para combinar as contagens de diferentes partições
def combOp(acc1, acc2):
    # Soma as contagens de duas partições
    return acc1 + acc2


def main():
    spark = SparkSession.builder.appName('Product Analysis') \
                                .config('spark.ui.port','4041') \
                                .getOrCreate()


    sc = spark.sparkContext

    path_file = './app/data/pricerunner_aggregate.csv'

    rdd = sc.textFile(path_file)

    # inicializando o valor de agregação com 0 
    initial_value = 0

    # usando aggregate para contar o numero total de linhas no RDD
    total_value = rdd.aggregate(initial_value, seqOp, combOp)

    print(total_value)

if __name__ == '__main__':
    main()

