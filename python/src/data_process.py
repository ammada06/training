import pandas as pd

def process_data(data_file):
    # Read data
    df = pd.read_csv(data_file)
    
    # Drop missing values
    df = df.dropna()
    
    # Perform certain operations
    # For example, calculate the sum of a column
    total = df['column'].sum()
    
    return total
