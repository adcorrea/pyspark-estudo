import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia

def main():
    spark = inicializa_spark()
    df = retorna_df_bolsa_familia(spark)

    df_select = df.select('ano_referencia', 'mes_referencia', 'valor_parcela')

    df_select.show()
    
if __name__ == '__main__':
    main()