import json
import time
import logging
import os
import pandas as pd
import numpy as np
from kafka import KafkaProducer
from datetime import datetime
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IEEETransactionProducer:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.topic = os.getenv('KAFKA_TOPIC', 'ieee_transactions')
        self.data_path = os.getenv('DATA_PATH', '/app/data/merged_ieee_cis.csv')
        
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_servers],
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            acks='all'
        )
        
        self.load_data()
        logger.info(f"Producer initialized for topic: {self.topic}")
    
    def load_data(self):
        """Load IEEE CIS dataset"""
        try:
            self.df = pd.read_csv(self.data_path)
            if 'isFraud' in self.df.columns:
                self.df = self.df.drop('isFraud', axis=1)
            
            logger.info(f"Loaded {len(self.df)} transactions from dataset")
            logger.info(f"Dataset columns: {list(self.df.columns)}")
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            self.df = self.create_sample_data()
    
    def create_sample_data(self):
        """Create sample IEEE CIS-like transactions if dataset not available"""
        logger.info("Creating sample IEEE CIS transactions...")
        
        n_samples = 1000
        current_time = int(time.time())
        
        sample_data = {
            'TransactionID': [f'T_{i:06d}' for i in range(n_samples)],
            'TransactionDT': np.random.randint(current_time - 86400, current_time, n_samples),
            'TransactionAmt': np.random.exponential(100, n_samples),
            'ProductCD': np.random.choice(['W', 'C', 'R', 'H', 'S'], n_samples),
            'card1': np.random.randint(1000, 20000, n_samples),
            'card2': np.random.choice([100, 200, 300, 400, 500, None], n_samples),
            'card3': np.random.choice([150, 185, None], n_samples),
            'card4': np.random.choice(['american express', 'discover', 'mastercard', 'visa'], n_samples),
            'card5': np.random.choice([100, 200, 300, None], n_samples),
            'card6': np.random.choice(['credit', 'debit', 'debit or credit'], n_samples),
            'addr1': np.random.randint(100, 500, n_samples),
            'addr2': np.random.randint(10, 100, n_samples),
            'dist1': np.random.uniform(0, 100, n_samples),
            'dist2': np.random.uniform(0, 100, n_samples),
            'P_emaildomain': np.random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', None], n_samples),
            'R_emaildomain': np.random.choice(['gmail.com', 'yahoo.com', 'outlook.com', None], n_samples),
            'DeviceType': np.random.choice(['desktop', 'mobile'], n_samples),
            'DeviceInfo': np.random.choice(['Windows', 'iOS Device', 'MacOS', 'Android'], n_samples),
        }
        
        
        for i in range(1, 39):
            col_name = f'id_{i:02d}'
            if i <= 11:  
                sample_data[col_name] = np.random.uniform(0, 1, n_samples)
            else:  
                sample_data[col_name] = np.random.choice(['Found', 'NotFound', None], n_samples)
        
        # Add M1-M9 features
        for i in range(1, 10):
            sample_data[f'M{i}'] = np.random.choice(['T', 'F', None], n_samples)
        
        # Add V1-V339 features (Vesta features)
        for i in range(1, 340):
            sample_data[f'V{i}'] = np.random.uniform(-5, 5, n_samples)
        
        # Add C1-C14 features (counting features)
        for i in range(1, 15):
            sample_data[f'C{i}'] = np.random.randint(0, 1000, n_samples)
        
        # Add D1-D15 features (timedelta features) 
        for i in range(1, 16):
            sample_data[f'D{i}'] = np.random.uniform(0, 1000, n_samples)
        
        return pd.DataFrame(sample_data)
    
    def simulate_realtime_transaction(self, base_transaction):
        """Modify transaction to simulate real-time data"""
        transaction = base_transaction.copy()
        
        # Update timestamp to current time
        transaction['TransactionDT'] = int(time.time())
        
        # Add some randomness to transaction amount
        if 'TransactionAmt' in transaction:
            base_amount = float(transaction['TransactionAmt'])
            # Add ±20% variation
            variation = random.uniform(0.8, 1.2)
            transaction['TransactionAmt'] = round(base_amount * variation, 2)
        
        # Randomly modify some categorical features to simulate different users
        if random.random() < 0.3:  # 30% chance to change card info
            transaction['card1'] = random.randint(1000, 20000)
            
        if random.random() < 0.2:  # 20% chance to change email domain
            domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
            transaction['P_emaildomain'] = random.choice(domains)
        
        # Convert numpy types to native Python types for JSON serialization
        for key, value in transaction.items():
            if isinstance(value, (np.integer, np.int64)):
                transaction[key] = int(value)
            elif isinstance(value, (np.floating, np.float64)):
                transaction[key] = float(value)
            elif pd.isna(value):
                transaction[key] = None
                
        return transaction
    
    def send_transaction_stream(self, rate_per_second=2):
        """Send transactions at specified rate"""
        logger.info(f"Starting transaction stream at {rate_per_second} TPS...")
        
        try:
            transaction_count = 0
            while True:
                
                idx = random.randint(0, len(self.df) - 1)
                base_transaction = self.df.iloc[idx].to_dict()
                
               
                transaction = self.simulate_realtime_transaction(base_transaction)
                
                
                future = self.producer.send(self.topic, transaction)
                
                # Log every 10 transactions
                transaction_count += 1
                if transaction_count % 10 == 0:
                    logger.info(f"Sent {transaction_count} transactions. "
                               f"Latest: {transaction.get('TransactionID')} - "
                               f"Amount: ${transaction.get('TransactionAmt', 0):.2f}")
                
                time.sleep(1.0 / rate_per_second)
                
        except KeyboardInterrupt:
            logger.info("Stopping transaction stream...")
        except Exception as e:
            logger.error(f"Error in transaction stream: {e}")
        finally:
            self.producer.flush()
            self.producer.close()
    
    def send_batch_test(self, batch_size=100):
        """Send a batch of test transactions"""
        logger.info(f"Sending batch of {batch_size} test transactions...")
        
        try:
            for i in range(batch_size):
                idx = random.randint(0, len(self.df) - 1)
                base_transaction = self.df.iloc[idx].to_dict()
                transaction = self.simulate_realtime_transaction(base_transaction)
                
                self.producer.send(self.topic, transaction)
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Sent {i + 1}/{batch_size} transactions")
            
            self.producer.flush()
            logger.info("Batch sending complete")
            
        except Exception as e:
            logger.error(f"Error in batch sending: {e}")

def main():
    producer = IEEETransactionProducer()
    
    mode = os.getenv('PRODUCER_MODE', 'stream')
    
    if mode == 'batch':
        batch_size = int(os.getenv('BATCH_SIZE', '100'))
        producer.send_batch_test(batch_size)
    else:
        rate = float(os.getenv('TRANSACTION_RATE', '2'))
        producer.send_transaction_stream(rate)

if __name__ == "__main__":
    main()