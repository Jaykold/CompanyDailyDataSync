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

    import pandas as pd

# Define the chunk size (number of rows per chunk)
chunk_size = 100000  # Adjust this based on your memory capacity

# Initialize an empty list to collect the results
chunks = []

# Use the chunksize parameter to load the data in smaller chunks
for chunk in pd.read_csv('path_to_your_file.csv', chunksize=chunk_size):
    # Extract the relevant columns
    relevant_chunk = chunk[['name', 'size', 'industry', 'locality']]
    
    # Append the result to the list of chunks
    chunks.append(relevant_chunk)

# Concatenate all the chunks into a single DataFrame
final_data = pd.concat(chunks, axis=0)

# Optionally, save the final output to a new CSV file
final_data.to_csv('output_file.csv', index=False)

# Check the first few rows of the final result
print(final_data.head())