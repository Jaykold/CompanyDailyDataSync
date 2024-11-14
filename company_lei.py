import requests
import pandas as pd
import re
from aiohttp import ClientSession
import asyncio
import math
from typing import List, Dict, Optional
from tqdm import tqdm


class Lei:
    def __init__(self):
        self.path = "forward_firm_universe.xlsx"
        self.api_url = 'https://api.gleif.org/api/v1/lei-records'
        self.batch_size = 60
        self.rate_limit_delay = 0.1
    
    def load_data(self):
        df = pd.read_excel(self.path)

        return df

    def clean_data(self):
        
        df = self.load_data()
        df.columns = df.columns.str.lower()

        df['cleaned_entity_name'] = df['entity_name'].apply(self.clean_company_name)
        df_duplicate = df.drop_duplicates(subset=['cleaned_entity_name'])
        df_cleaned = df_duplicate.drop(columns=['lei'])
        df_cleaned = df_cleaned[['cleaned_entity_name', 'entity_name']]
        
        return df_cleaned

    
    def clean_company_name(self, name):
        
        # Remove common words and punctuation
        name = re.sub(r"\b(Co|Ltd|Corp|LLC|Company|Inc)\b", "", name, flags=re.IGNORECASE)
        name = re.sub(r"[^\w\s]", "", name)
        name = name.strip()
        return name
    
    async def fetch_lei(self, session: ClientSession, company_name: str)-> Optional[str]:
        """Fetch LEI for a single company using aiohttp session."""
        params = {
            "filter[entity.legalName]": company_name,
            "page[size]": 1
        }
        try:
            async with session.get(self.api_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    results = data.get('data', [])
                    if results:
                        return results[0]['attributes']['lei']
                return None
        except Exception as e:
            print(f"Error fetching LEI for {company_name}: {str(e)}")
            return None
        
    async def get_leis_batch(self, session: ClientSession, company_names: List[str]) -> Dict[str, Optional[str]]:
        """Process a batch of company names concurrently."""
        tasks = []
        for company in company_names:
            await asyncio.sleep(self.rate_limit_delay)
            tasks.append(self.fetch_lei(session, company))

        results = await asyncio.gather(*tasks)
        return dict(zip(company_names, results))
    
    async def process_companies_async(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process all companies asynchronously."""
        df_copy = df.copy()
        n_companies = len(df_copy)
        n_batches = math.ceil(n_companies / self.batch_size)

        all_results = {}
        async with ClientSession() as session:
            with tqdm(total=n_batches, desc="Processing batches") as pbar:
                for i in range(n_batches):
                    start_idx = i * self.batch_size
                    end_idx = min((i + 1) * self.batch_size, n_companies)

                    batch_companies = df_copy['entity_name'].iloc[start_idx:end_idx].tolist()
                    batch_results = await self.get_leis_batch(session, batch_companies)
                    all_results.update(batch_results)

                    pbar.update(1)

        df_copy['lei'] = df_copy['cleaned_entity_name'].map(all_results)
        return df_copy

    def process_companies_in_batches(self, df: pd.DataFrame) -> pd.DataFrame:
        """Wrapper function to handle async execution."""
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.process_companies_async(df))
    
if __name__ == "__main__":
    lei = Lei()
    df = lei.clean_data()
    result_df = lei.process_companies_in_batches(df)