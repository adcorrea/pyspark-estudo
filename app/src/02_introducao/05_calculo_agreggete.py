from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import countDistinct

spark = SparkSession.builder.appName('Calculate') \
                            .config('spark.ui.port','4041') \
                            .getOrCreate()


path_file = './app/data/pricerunner_aggregate.csv'

df = spark.read.csv(path_file, header=True, inferSchema=True)

# exclusão de linhas nulas
df_clean = df.na.drop()

col_names = df_clean.schema.names

for col_name in col_names:
    new_col_name = col_name.strip()
    df_clean = df_clean.withColumnRenamed(col_name, new_col_name)


# Agrupando por categoria e contando os produtos
categoria_distribuicao = df_clean.groupBy('Category Label').agg(count('Product ID').alias('Count')) \
                                    .orderBy('Count', ascending=False)


# Visualizando o resultado
categoria_distribuicao.show()

# Identificando os Comerciantes com maoires ofertas
comerciantes_top = df_clean.groupBy('Merchant ID').agg(count('Product ID').alias('Total Products')) \
                                            .orderBy('Total Products', ascending=False)

comerciantes_top.show()

# Contando a quantidade de titulos de produtos em cada categoria
diversidade_categoria = df_clean.groupBy('Category Label').agg(countDistinct('Product Title').alias('Unique Product Titles')) 

diversidade_categoria.show()