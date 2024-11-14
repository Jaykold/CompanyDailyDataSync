import json
import os
from dotenv import load_dotenv
from peopledatalabs import PDLPY

load_dotenv()

class Config:
    def __init__(self):
        self.data_path = 'forward_firm_universe.xlsx'
        self.gleif_api_url = 'https://api.gleif.org/api/v1/lei-records'
        self.pdl_url = "https://api.peopledatalabs.com/v5/company/enrich"
        self.pdl_api_key = os.getenv("PDL_API_KEY")
        self.batch_size = 60
        self.rate_limit_delay = 0.1
        self.company_name = 'NNPC'

    def get_gleif_data(self):
# Create a client, specifying your API key
        client = PDLPY(
            api_key= self.pdl_api_key
        )

        # Create an SQL query
        SQL_QUERY = f"SELECT * FROM company WHERE (name = '{self.company_name}')"

        # Create a parameters JSON object
        params = {
        'dataset': 'all',
        'sql': SQL_QUERY,
        'size': 1,
        'pretty': "True"
        }

        # Pass the parameters object to the Company Search API
        response = client.company.search(**params).json()

        # Check for successful response
        if response["status"] == 200:
            data = response['data']
            if data:
                company = data[0]

                return {
                    'entity_type': company.get('type', 'N/A'),
                    'industry_classification': company.get('industry', 'N/A'),
                    'company_size': company.get('size', 'N/A'),
                }
        else:
            print("NOTE. The carrier pigeons lost motivation in flight. See error and try again.")
            print("Error:", response)

if __name__ == "__main__":
    config = Config()
    print(config.get_gleif_data())