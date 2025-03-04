import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia
import matplotlib.pyplot as plt
import seaborn as sns

def main():

    spark = inicializa_spark()

    df = retorna_df_bolsa_familia(spark=spark)

    valores_parcelas = df.select('valor_parcela').rdd.flatMap(lambda x: x).collect()

    plt.figure(figsize=(10, 6))
    sns.histplot(valores_parcelas, bins=30, kde=True)
    plt.title('Distribuicao dos valores das parcelas do bolsa família Junho/2023')
    plt.xlabel('Valor da Parcela')
    plt.ylabel('Frequencia')
    plt.show()
    
if __name__ == '__main__':
    main()