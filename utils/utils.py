import pandas as pd
from datetime import date
from utils import logging

def save_to_excel(df: pd.DataFrame, filepath: str):
    '''Saves dataset to Excel file'''
    df.to_excel(filepath, index=False)


def save_to_csv(dataset:pd.DataFrame, directory="data", filename_prefix="summary_stats"):
    '''This function saves summary statistics of the dataset daily'''
    if not isinstance(dataset, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")
    current_date = date.today().strftime("%Y_%m_%d")
    
    # Create the full filename
    filename = f"{directory}/{filename_prefix}_{current_date}.csv"
    
    # Save the DataFrame to CSV
    dataset.to_csv(filename, index=False)
    logging.info(f"DataFrame saved to {filename}")

# Function to generate a completeness summary report
def generate_completeness_report(df: pd.DataFrame):
    report = []
    columns = ["lei", "industry_classification", "n_employees", "entity_type"]

    for col in columns:
        total_count = len(df)
        non_missing_count = df[col].notnull().sum()
        missing_count = df[col].isnull().sum()
        completeness_percentage = (non_missing_count / total_count) * 100

        report.append({
            "attribute": col,
            "non-Missing Count": non_missing_count,
            "missing Count": missing_count,
            "completeness (%)": round(completeness_percentage, 2)
        })

    report_df = pd.DataFrame(report)
    return report_df
