import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia

def main():
    spark = inicializa_spark()

    df = retorna_df_bolsa_familia(spark=spark)

    # calculando estatisticas descritivas sobre valor_parcela
    df.describe('valor_parcela').show()

    # Soma total de pagamentos por UF
    df.groupBy('UF').sum('valor_parcela').show()

if __name__ == '__main__':
    main()