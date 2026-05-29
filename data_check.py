import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(
    filename=f'quality_report_{datetime.now().strftime("%Y%m%d")}.log', 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_data_quality_check(data_path):
    logging.info("--- Starting Daily Data Quality Check ---")
    
    df = pd.read_csv(data_path)
    
    missing_values = df.isnull().sum().sum()
    if missing_values > 0:
        logging.warning(f"ALERT: Found {missing_values} missing values in pipeline data.")
    else:
        logging.info("Check Passed: No missing values detected.")
        
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        logging.warning(f"ALERT: Found {duplicates} duplicate transaction rows.")
    
    fraud_count = df[df['Class'] == 1].shape[0]
    total_count = df.shape[0]
    
    logging.info(f"Performance Report: Processed {total_count} transactions.")
    logging.info(f"Fraud Detection Ratio: {fraud_count}/{total_count} ({(fraud_count/total_count)*100:.2f}%)")
    logging.info("--- Quality Check Complete ---")

if __name__ == "__main__":
    run_data_quality_check("creditcard.csv")