
import pandas as pd
import matplotlib.pyplot as plt
import logging
import os

# Create a logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Set up logging
logging.basicConfig(filename='logs/data_processing.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def read_csv_in_chunks(file_path, chunk_size=10000):
    """
    Reads a large CSV file in chunks to optimize memory usage.
    """
    logging.info(f"Reading CSV file: {file_path}")
    try:
        chunk_list = []
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk_list.append(chunk)
        df = pd.concat(chunk_list, ignore_index=True)
        logging.info("Successfully read CSV file.")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return None

def clean_data(df):
    """
    Cleans the DataFrame by handling missing values.
    """
    logging.info("Starting data cleaning.")
    # Simple strategy: fill missing rental_amount with the mean
    if 'rental_amount' in df.columns and df['rental_amount'].isnull().any():
        mean_rental = df['rental_amount'].mean()
        df['rental_amount'].fillna(mean_rental, inplace=True)
        logging.info("Filled missing rental_amount with the mean.")
    
    # Drop rows with any other missing values
    if df.isnull().values.any():
        df.dropna(inplace=True)
        logging.info("Dropped rows with missing values.")
    
    logging.info("Data cleaning complete.")
    return df

def get_top_10_equipment(df):
    """
    Calculates the top 10 farm equipment by total rental amount.
    """
    logging.info("Calculating top 10 rental equipment.")
    if 'rental_amount' in df.columns and 'farm_equipment_name' in df.columns:
        top_10 = df.groupby('farm_equipment_name')['rental_amount'].sum().nlargest(10)
        logging.info("Successfully calculated top 10 equipment.")
        return top_10
    else:
        logging.error("Required columns for analysis are missing.")
        return None

def create_visualization(top_10, output_path='top_10_equipment.png'):
    """
    Creates and saves a bar chart of the top 10 equipment.
    """
    logging.info("Creating visualization.")
    if top_10 is not None:
        plt.figure(figsize=(12, 8))
        top_10.plot(kind='bar')
        plt.title('Top 10 Rented Farm Equipment by Rental Amount')
        plt.xlabel('Farm Equipment')
        plt.ylabel('Total Rental Amount')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(output_path)
        logging.info(f"Visualization saved to {output_path}")
        print(f"Visualization saved to {output_path}")
    else:
        logging.warning("No data to visualize.")

def main():
    """
    Main function to run the data processing pipeline.
    """
    logging.info("Starting data processing pipeline.")
    
    # 1. Read data
    df = read_csv_in_chunks('rental_data.csv')
    
    if df is not None:
        # 2. Clean data
        df = clean_data(df)
        
        # 3. Generate insights
        top_10 = get_top_10_equipment(df)
        
        if top_10 is not None:
            print("Top 10 Rented Farm Equipment:")
            print(top_10)
        
        # 4. Create visualization
        create_visualization(top_10)
        
    logging.info("Data processing pipeline finished.")

if __name__ == "__main__":
    main()
