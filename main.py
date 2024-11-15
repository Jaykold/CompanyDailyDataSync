import pandas as pd
from utils import save_to_excel, generate_completeness_report, logging
from src import Lei, Pdl

def main():
    lei = Lei()
    df = lei.clean_data()
    result_df = lei.process_companies_in_batches(df)
    pdl = Pdl()
    enriched_df = pdl.enrich_dataframe(result_df)
    logging.info('Data enrichment completed!')
    df = generate_completeness_report(enriched_df)
    save_to_excel(df, pdl.filename)
    logging.info("Data saved to excel file")

if __name__=="__main__":
    main()