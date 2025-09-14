# fraud_detector.py - IEEE CIS Fraud Detection Service
import json
import logging
import time
import os
import traceback
from datetime import datetime
from typing import Dict
import numpy as np
import pandas as pd
import joblib
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from sklearn.preprocessing import LabelEncoder
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
TRANSACTIONS_PROCESSED = Counter('ieee_transactions_processed_total', 'Total IEEE CIS transactions processed')
FRAUD_DETECTED = Counter('ieee_fraud_detected_total', 'Total IEEE CIS fraud cases detected')
PREDICTION_TIME = Histogram('ieee_prediction_duration_seconds', 'Time spent on IEEE CIS prediction')
FRAUD_RATE = Gauge('ieee_current_fraud_rate', 'Current IEEE CIS fraud rate')
MODEL_SCORE_HISTOGRAM = Histogram('ieee_fraud_score_distribution', 'Distribution of fraud scores')

class IEEEFraudDetectionService:
    def __init__(self):
        self.model = None
        self.feature_cols = None
        self.label_encoders = {}
        self.redis_client = None
        self.db_connection = None
        self.kafka_consumer = None
        self.kafka_producer = None
        self.fraud_threshold = 0.5  
        s
        self.categorical_features = [
            'ProductCD', 'card1', 'card2', 'card3', 'card4', 'card5', 'card6',
            'addr1', 'addr2', 'P_emaildomain', 'R_emaildomain', 'DeviceType',
            'DeviceInfo', 'id_12', 'id_13', 'id_14', 'id_15', 'id_16', 'id_17',
            'id_18', 'id_19', 'id_20', 'id_21', 'id_22', 'id_23', 'id_24',
            'id_25', 'id_26', 'id_27', 'id_28', 'id_29', 'id_30', 'id_31',
            'id_32', 'id_33', 'id_34', 'id_35', 'id_36', 'id_37', 'id_38',
            'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9'
        ]
        
        self.setup_connections()
        self.load_model()
        
    def setup_connections(self):
        """Setup all external connections"""
        # Redis connection
        try:
            redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
            self.redis_client = redis.from_url(redis_url)
            self.redis_client.ping()
            logger.info("Redis connection established")
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
        
        # PostgreSQL connection
        try:
            db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:fraudpassword123@localhost:5432/ieee_fraud_db')
            self.db_connection = psycopg2.connect(db_url)
            self.create_tables()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
        
        # Kafka setup
        try:
            kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            
            self.kafka_consumer = KafkaConsumer(
                os.getenv('KAFKA_INPUT_TOPIC', 'ieee_transactions'),
                bootstrap_servers=[kafka_servers],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='ieee-fraud-detector-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=[kafka_servers],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                acks='all',
                retries=3
            )
            
            logger.info("Kafka connections established")
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")
    
    def create_tables(self):
        """Create database tables for IEEE CIS dataset"""
        cursor = self.db_connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ieee_transactions (
                TransactionID VARCHAR(50) PRIMARY KEY,
                TransactionDT BIGINT,
                TransactionAmt DECIMAL(12,4),
                ProductCD VARCHAR(10),
                card1 INTEGER,
                card2 DECIMAL(8,2),
                card3 DECIMAL(8,2),
                card4 VARCHAR(20),
                card5 DECIMAL(8,2),
                card6 VARCHAR(20),
                addr1 DECIMAL(8,2),
                addr2 DECIMAL(8,2),
                dist1 DECIMAL(8,4),
                dist2 DECIMAL(8,4),
                P_emaildomain VARCHAR(50),
                R_emaildomain VARCHAR(50),
                DeviceType VARCHAR(20),
                DeviceInfo VARCHAR(100),
                raw_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ieee_predictions (
                id SERIAL PRIMARY KEY,
                TransactionID VARCHAR(50),
                fraud_probability FLOAT,
                is_fraud_predicted BOOLEAN,
                processing_time_ms FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.db_connection.commit()
        cursor.close()
        logger.info("IEEE CIS database tables created/verified")
    
    def load_model(self):
        """Load the trained IEEE CIS fraud detection model"""
        try:
            model_path = os.getenv('MODEL_PATH', '/app/models/xgb_model.pkl')
            features_path = os.getenv('FEATURES_PATH', '/app/models/feature_cols.pkl')
            
            self.model = joblib.load(model_path)
            self.feature_cols = joblib.load(features_path)
            
            logger.info(f"IEEE CIS model loaded with {len(self.feature_cols)} features")
        except Exception as e:
            logger.error(f"Model loading failed: {e}")
            raise
    
    def engineer_features(self, transaction_data: Dict) -> pd.DataFrame:
        """Convert raw transaction JSON into ML features"""
        df = pd.DataFrame([transaction_data])
        
        for col in self.categorical_features:
            if col in df.columns:
                if col not in self.label_encoders:
                    le = LabelEncoder()
                    df[col] = df[col].astype(str).fillna("unknown")
                    le.fit(df[col])
                    self.label_encoders[col] = le
                df[col] = self.label_encoders[col].transform(df[col].astype(str).fillna("unknown"))
        

        for col in self.feature_cols:
            if col not in df.columns:
                df[col] = 0
        
        return df[self.feature_cols]
    
    def predict_fraud(self, transaction: Dict) -> Dict:
        """Run fraud prediction on a single transaction"""
        start_time = time.time()
        TRANSACTIONS_PROCESSED.inc()
        
        try:
            features = self.engineer_features(transaction)
            prob = float(self.model.predict_proba(features)[0][1])
            is_fraud = prob >= self.fraud_threshold
            

            if is_fraud:
                FRAUD_DETECTED.inc()
            MODEL_SCORE_HISTOGRAM.observe(prob)
            
            result = {
                "TransactionID": transaction.get("TransactionID"),
                "fraud_probability": prob,
                "is_fraud_predicted": is_fraud,
                "processing_time_ms": (time.time() - start_time) * 1000
            }
            return result
        
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            traceback.print_exc()
            return {
                "TransactionID": transaction.get("TransactionID"),
                "fraud_probability": 0.0,
                "is_fraud_predicted": False,
                "processing_time_ms": (time.time() - start_time) * 1000,
                "error": str(e)
            }
    
    def save_to_database(self, transaction: Dict, prediction: Dict):
        """Save transaction and prediction to PostgreSQL"""
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("""
                INSERT INTO ieee_transactions (TransactionID, TransactionDT, TransactionAmt, 
                                        ProductCD, card1, card2, addr1, P_emaildomain, 
                                        DeviceInfo, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (TransactionID) DO NOTHING
            """, (
                transaction.get('TransactionID'),
                transaction.get('TransactionDT'),
                transaction.get('TransactionAmt'),
                transaction.get('ProductCD'),
                transaction.get('card1'),
                transaction.get('card2'),
                transaction.get('addr1'),
                transaction.get('P_emaildomain'),
                transaction.get('DeviceInfo'),
                json.dumps(transaction)
            ))
            
            cursor.execute("""
                INSERT INTO ieee_predictions (TransactionID, fraud_probability, is_fraud_predicted, processing_time_ms)
                VALUES (%s, %s, %s, %s)
            """, (
                prediction.get('TransactionID'),
                prediction.get('fraud_probability'),
                prediction.get('is_fraud_predicted'),
                prediction.get('processing_time_ms')
            ))
            
            self.db_connection.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Database save error: {e}")
            self.db_connection.rollback()
    
    def run(self):
        """Main processing loop"""
        logger.info("Starting IEEE CIS Fraud Detection Service...")
        start_http_server(8000)
        logger.info("Metrics server started on port 8000")
        
        try:
            for message in self.kafka_consumer:
                transaction = message.value
                logger.info(f"Processing transaction: {transaction.get('TransactionID')}")
                
                prediction = self.predict_fraud(transaction)
                self.save_to_database(transaction, prediction)
                self.kafka_producer.send('fraud_predictions', prediction)
                
                fraud_count = FRAUD_DETECTED._value.get()
                total_count = TRANSACTIONS_PROCESSED._value.get()
                if total_count > 0:
                    FRAUD_RATE.set(fraud_count / total_count)
                
                if prediction.get('fraud_probability', 0) > 0.8:
                    logger.warning(f"HIGH RISK: {transaction.get('TransactionID')} - "
                                   f"Score: {prediction.get('fraud_probability'):.3f}")
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.kafka_consumer.close()
            self.kafka_producer.close()
            self.db_connection.close()

if __name__ == "__main__":
    detector = IEEEFraudDetectionService()
    detector.run()
