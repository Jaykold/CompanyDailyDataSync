import os
from typing import Dict, List
import math
import asyncio
import pandas as pd
from aiohttp import ClientSession, ClientError
from peopledatalabs import PDLPY
from tqdm import tqdm
#from src import Lei
from utils import save_to_excel, logging

class Pdl:
    def __init__(self):
        self.pdl_url = "https://api.peopledatalabs.com/v5/company/enrich"
        self.pdl_api_key = os.getenv("PDL_API_KEY2")
        self.batch_size = 50
        self.rate_limit_delay = 0.2
        self.filename = "data/forward_firm_universe.xlsx"
        self.is_using_sql_query = False

    async def fetch_company_info(self, session:ClientSession, company_name: str)->Dict[str, str]:
        params = {
            "name": company_name,
            "pretty": "true",
            "api_key": self.pdl_api_key
        }
        try:
            async with session.get(self.pdl_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'entity_type': data.get('type', 'N/A'),
                        'industry_classification': data.get('industry', 'N/A'),
                        'n_employees': data.get('employee_count', 'N/A'),
                    }
                elif response.status == 402:
                    return {
                        'entity_type': 'N/A',
                        'industry_classification': 'N/A',
                        'n_employees': 'N/A'
                    }
                else:
                    logging.error(f"Failed to retrieve data for {company_name} (HTTP {response.status})")
                    return {
                        'entity_type': 'N/A',
                        'industry_classification': 'N/A',
                        'n_employees': 'N/A'
                    }
        except ClientError as e:
            logging.error(f"Error fetching company info for {company_name}: {str(e)}")
            return {'entity_type': 'N/A', 'industry_classification': 'N/A', 'company_size': 'N/A'}
                
    # Function to process data in batches
    async def fetch_all_company_info(self, company_names: List[str]):
        results = []
        n_companies = len(company_names)
        n_batches = math.ceil(n_companies / self.batch_size)

        async with ClientSession() as session:
            with tqdm(total=n_batches, desc="Processing batches") as pbar:
                for i in range(0, len(company_names), self.batch_size):
                    batch = company_names[i:i+self.batch_size]
                    tasks = [self.fetch_company_info(session, company) for company in batch]
                    batch_results = await asyncio.gather(*tasks)
                    results.extend(batch_results)

                    pbar.update(1)
        return results

    def enrich_dataframe(self, df: pd.DataFrame):

        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
        
        # Get unique company names and their index in the dataframe
        company_names = df['entity_name'].tolist()
        
        # Fetch data concurrently
        results = asyncio.run(self.fetch_all_company_info(company_names))

        if len(results) != len(company_names):
            raise ValueError("The number of results does not match the number of company names.")
        
        # Create new columns in the DataFrame and populate with fetched data
        df['entity_type'] = [result['entity_type'] for result in results]
        df['industry_classification'] = [result['industry_classification'] for result in results]
        df['n_employees'] = [result['n_employees'] for result in results]

        return df

if __name__=="__main__":
    # lei = Lei()
    # df = lei.clean_data()
    # result_df = lei.process_companies_in_batches(df)
    pdl = Pdl()
    data = pd.DataFrame({'entity_name': ['Microsoft', 'Google', 'Tesla']})
    #df = pd.read_csv("result_df.csv")
    enriched_df = pdl.enrich_dataframe(data)
    save_to_excel(data, pdl.filename)