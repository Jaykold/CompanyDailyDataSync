import os
from typing import Dict, List
import math
import asyncio
import pandas as pd
from aiohttp import ClientSession, ClientError
from tqdm import tqdm

from utils import logging

class Pdl:
    def __init__(self):
        self.pdl_url = "https://api.peopledatalabs.com/v5/company/enrich"
        self.pdl_api_key = os.getenv("PDL_API_KEY")
        self.batch_size = 60
        self.rate_limit_delay = 0.1
        self.filename = "data/forward_firm_universe.xlsx"

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
                elif response.status == 402: # Rate limit
                    logging.warning(f"Rate limit reached while fetching data for {company_name}.")
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
            return {'entity_type': 'N/A', 'industry_classification': 'N/A', 'n_employees': 'N/A'}
                
    # Function to process data in batches
    async def fetch_all_company_info(self, company_names: List[str]):
        results = []
        n_companies = len(company_names)
        n_batches = math.ceil(n_companies / self.batch_size)

        async with ClientSession() as session:
            with tqdm(total=n_batches, desc="Processing batches") as pbar:
                for i in range(n_batches):
                    start = i * self.batch_size
                    end = min(start + self.batch_size, n_companies)
                    batch = company_names[start:end]
                    tasks = [self.fetch_company_info(session, company) for company in batch]
                    batch_results = await asyncio.gather(*tasks)
                    results.extend(batch_results)

                    pbar.update(1)
        return results

    def enrich_dataframe(self, df: pd.DataFrame):

        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
        
        for col in ['entity_type', 'industry_classification', 'n_employees']:
            if col not in df.columns:
                df[col] = 'N/A'

        if 'entity_type' in df.columns:
            companies_to_enrich = df[df['entity_type'].isnull()]
            if companies_to_enrich.empty:
                logging.info("All companies have been enriched.")
                return df
        else:
            logging.warning("No 'entity_type' column found in the DataFrame. Enriching all companies.")
            companies_to_enrich = df

        # Get unique company names and their index in the dataframe
        company_names = companies_to_enrich['entity_name'].tolist()
        
        # Fetch data concurrently
        results = asyncio.run(self.fetch_all_company_info(company_names))

        if len(results) != len(company_names):
            raise ValueError("Results count does not match company names.")
        
        # Update the DataFrame with the new data
        companies_to_enrich.loc[:, 'entity_type'] = [result['entity_type'] for result in results]
        companies_to_enrich.loc[:, 'industry_classification'] = [result['industry_classification'] for result in results]
        companies_to_enrich.loc[:, 'n_employees'] = [result['n_employees'] for result in results]

        # Update the original DataFrame
        df.update(companies_to_enrich)

        return df