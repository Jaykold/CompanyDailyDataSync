import os
import pytest
import pandas as pd
from company_pdl import enrich_dataframe, fetch_company_info  # Adjust the import path as needed

@pytest.mark.asyncio
async def test_enrich_dataframe(mocker):
    # Sample DataFrame to test with
    api_key = os.getenv("PDL_API_KEY")
    url = "https://api.peopledatalabs.com/v5/company/enrich"
    
    test_data = {'Company Name': ['Microsoft', 'Google', 'Tesla']}
    df = pd.DataFrame(test_data)

    # Define mock return values for each company
    mock_fetch_company_info = mocker.patch('company_enrichment.fetch_company_info', side_effect=[
        {'Entity Type': 'Public', 'Industry Classification': 'Technology', 'Company Size': '10,000+'},
        {'Entity Type': 'Public', 'Industry Classification': 'Technology', 'Company Size': '100,000+'},
        {'Entity Type': 'Public', 'Industry Classification': 'Automotive', 'Company Size': '10,000+'}
    ])

    # Run the function to enrich the DataFrame
    enriched_df = enrich_dataframe(df, url, api_key)

    # Check that new columns are added
    assert 'Entity Type' in enriched_df.columns
    assert 'Industry Classification' in enriched_df.columns
    assert 'Company Size' in enriched_df.columns

    # Check that the values match the mock data
    assert enriched_df.loc[0, 'entity_type'] == 'Public'
    assert enriched_df.loc[0, 'industry_classification'] == 'Technology'
    assert enriched_df.loc[0, 'company_size'] == '10,000+'
    assert enriched_df.loc[1, 'entity_type'] == 'Public'
    assert enriched_df.loc[1, 'industry_classification'] == 'Technology'
    assert enriched_df.loc[1, 'company_size'] == '100,000+'
    assert enriched_df.loc[2, 'entity_type'] == 'Public'
    assert enriched_df.loc[2, 'industry_classification'] == 'Automotive'
    assert enriched_df.loc[2, 'company_size'] == '10,000+'

    # Verify that fetch_company_info was called the correct number of times
    assert mock_fetch_company_info.call_count == len(df)