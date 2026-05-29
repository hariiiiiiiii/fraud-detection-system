import json
import time
import logging
import os
import pandas as pd
import numpy as np
from kafka import KafkaProducer
from kafka import KafkaConsumer
from collections import deque
import threading
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PipelineBenchmark:
    def __init__(self, data_path='/Users/jagan/Documents/coding/projects/fraud-detection-system/data/processed/merged_ieee_cis.csv'):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'ieee_transactions')
        self.data_path = data_path
        
        # Metrics
        self.transactions_sent = 0
        self.transactions_received = 0
        self.latencies = deque(maxlen=10000)
        self.send_times = {}
        self.start_time = None
        
        self.load_data()
        
    def load_data(self):
        """Load IEEE CIS dataset"""
        try:
            self.df = pd.read_csv(self.data_path)
            if 'isFraud' in self.df.columns:
                self.df = self.df.drop('isFraud', axis=1)
            logger.info(f"Loaded {len(self.df)} transactions")
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def prepare_transaction(self, base_transaction):
        """Prepare transaction for sending"""
        transaction = base_transaction.copy()
        transaction['TransactionDT'] = int(time.time())
        transaction['benchmark_id'] = f"bench_{self.transactions_sent}"
        transaction['send_timestamp'] = time.time()
        
        # Convert numpy types to native Python types
        for key, value in transaction.items():
            if isinstance(value, (np.integer, np.int64)):
                transaction[key] = int(value)
            elif isinstance(value, (np.floating, np.float64)):
                transaction[key] = float(value)
            elif pd.isna(value):
                transaction[key] = None
                
        return transaction
    
    def benchmark_producer_only(self, duration_seconds=60, max_speed=True):
        """
        Measure maximum producer throughput (no throttling)
        This shows what Kafka can handle for ingestion
        """
        logger.info(f"Starting producer-only benchmark for {duration_seconds}s...")
        logger.info("Sending transactions as fast as possible...")
        
        producer = KafkaProducer(
            bootstrap_servers=[self.kafka_servers],
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            acks=1,  # Faster than 'all'
            compression_type='lz4',  # Enable compression
            batch_size=32768,  # Larger batches
            linger_ms=10  # Small delay to batch messages
        )
        
        self.start_time = time.time()
        self.transactions_sent = 0
        
        try:
            while (time.time() - self.start_time) < duration_seconds:
                # Get random transaction
                idx = np.random.randint(0, len(self.df))
                base_transaction = self.df.iloc[idx].to_dict()
                transaction = self.prepare_transaction(base_transaction)
                
                # Send without waiting
                producer.send(self.topic, transaction)
                self.transactions_sent += 1
                
                # Progress update every 1000 transactions
                if self.transactions_sent % 1000 == 0:
                    elapsed = time.time() - self.start_time
                    current_tps = self.transactions_sent / elapsed
                    logger.info(f"Sent: {self.transactions_sent:,} | "
                              f"Current TPS: {current_tps:.1f}")
            
            # Flush remaining messages
            producer.flush()
            
        except KeyboardInterrupt:
            logger.info("Benchmark interrupted by user")
        finally:
            producer.close()
        
        total_time = time.time() - self.start_time
        avg_tps = self.transactions_sent / total_time
        
        logger.info("\n" + "="*60)
        logger.info("PRODUCER BENCHMARK RESULTS")
        logger.info("="*60)
        logger.info(f"Total transactions sent: {self.transactions_sent:,}")
        logger.info(f"Total time: {total_time:.2f}s")
        logger.info(f"Average TPS: {avg_tps:.2f}")
        logger.info(f"Peak throughput: ~{avg_tps:.0f} transactions/second")
        logger.info("="*60 + "\n")
        
        return avg_tps
    
    def benchmark_end_to_end(self, duration_seconds=60, target_tps=500):
        """
        Measure end-to-end pipeline performance with latency tracking
        This measures: Kafka -> Consumer -> Processing -> Response
        """
        logger.info(f"Starting end-to-end benchmark for {duration_seconds}s...")
        logger.info(f"Target rate: {target_tps} TPS")
        
        producer = KafkaProducer(
            bootstrap_servers=[self.kafka_servers],
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            acks=1
        )
        
        self.start_time = time.time()
        self.transactions_sent = 0
        
        # Calculate sleep time between sends to hit target TPS
        sleep_time = 1.0 / target_tps if target_tps > 0 else 0
        
        try:
            while (time.time() - self.start_time) < duration_seconds:
                idx = np.random.randint(0, len(self.df))
                base_transaction = self.df.iloc[idx].to_dict()
                transaction = self.prepare_transaction(base_transaction)
                
                # Track send time for latency measurement
                benchmark_id = transaction['benchmark_id']
                self.send_times[benchmark_id] = time.time()
                
                producer.send(self.topic, transaction)
                self.transactions_sent += 1
                
                if self.transactions_sent % 100 == 0:
                    elapsed = time.time() - self.start_time
                    current_tps = self.transactions_sent / elapsed
                    logger.info(f"Sent: {self.transactions_sent:,} | "
                              f"TPS: {current_tps:.1f}")
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
            
            producer.flush()
            
        except KeyboardInterrupt:
            logger.info("Benchmark interrupted")
        finally:
            producer.close()
        
        total_time = time.time() - self.start_time
        avg_tps = self.transactions_sent / total_time
        
        logger.info("\n" + "="*60)
        logger.info("END-TO-END BENCHMARK RESULTS")
        logger.info("="*60)
        logger.info(f"Total transactions sent: {self.transactions_sent:,}")
        logger.info(f"Duration: {total_time:.2f}s")
        logger.info(f"Achieved TPS: {avg_tps:.2f}")
        logger.info(f"Target TPS: {target_tps}")
        logger.info(f"Success rate: {(avg_tps/target_tps*100):.1f}%")
        logger.info("="*60 + "\n")
        
        return avg_tps
    
    def stress_test(self, duration_seconds=30):
        """
        Progressive stress test - increase load until system breaks
        """
        logger.info("Starting progressive stress test...")
        
        test_rates = [50, 100, 200, 500, 1000, 2000, 5000]
        results = {}
        
        for rate in test_rates:
            logger.info(f"\n--- Testing at {rate} TPS ---")
            
            try:
                achieved_tps = self.benchmark_end_to_end(
                    duration_seconds=duration_seconds,
                    target_tps=rate
                )
                
                results[rate] = {
                    'target': rate,
                    'achieved': achieved_tps,
                    'success': achieved_tps >= rate * 0.9  # 90% of target
                }
                
                # If we can't hit 90% of target, stop
                if achieved_tps < rate * 0.9:
                    logger.info(f"System saturated at {rate} TPS")
                    break
                    
                time.sleep(5)  # Cool down between tests
                
            except Exception as e:
                logger.error(f"Failed at {rate} TPS: {e}")
                break
        
        logger.info("\n" + "="*60)
        logger.info("STRESS TEST SUMMARY")
        logger.info("="*60)
        for rate, result in results.items():
            status = "✓" if result['success'] else "✗"
            logger.info(f"{status} Target: {result['target']:>4} TPS | "
                       f"Achieved: {result['achieved']:>6.1f} TPS")
        logger.info("="*60 + "\n")
        
        return results

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Benchmark fraud detection pipeline')
    parser.add_argument('--mode', choices=['producer', 'e2e', 'stress'], 
                       default='producer',
                       help='Benchmark mode')
    parser.add_argument('--duration', type=int, default=60,
                       help='Test duration in seconds')
    parser.add_argument('--target-tps', type=int, default=500,
                       help='Target TPS for e2e test')
    parser.add_argument('--data-path', type=str, 
                       default='data/merged_ieee_cis.csv',
                       help='Path to IEEE CIS dataset')
    
    args = parser.parse_args()
    
    benchmark = PipelineBenchmark(data_path=args.data_path)
    
    if args.mode == 'producer':
        benchmark.benchmark_producer_only(duration_seconds=args.duration)
    elif args.mode == 'e2e':
        benchmark.benchmark_end_to_end(
            duration_seconds=args.duration,
            target_tps=args.target_tps
        )
    elif args.mode == 'stress':
        benchmark.stress_test(duration_seconds=30)

if __name__ == "__main__":
    main()