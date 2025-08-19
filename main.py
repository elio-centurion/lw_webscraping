from src.ingestion.ingestion import ingestion
from src.load.loading import loading_raw, loading
from src.transformation.transformations_news import transformations


def main():
    all_news=ingestion()
    df_clean,df_raw=transformations(all_news)
    loading_raw(df_raw)
    loading(df_clean)

if __name__=='__main__':
    main()
