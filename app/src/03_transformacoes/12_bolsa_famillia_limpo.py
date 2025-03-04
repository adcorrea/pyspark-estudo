# main.py
import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia




def main():
    spark = inicializa_spark()
    df = retorna_df_bolsa_familia(spark)

    df.show()

if __name__ == '__main__':
    main()