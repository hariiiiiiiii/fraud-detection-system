# Real-Time Fraud Detection System

This repository contains an end-to-end streaming pipeline for detecting fraudulent transactions in real-time. The system simulates a stream of financial transactions, processes them with a machine learning model, and provides a monitoring dashboard to visualize key metrics.

## 🏗️ System Architecture

The pipeline is designed with a scalable, containerized architecture using modern data engineering tools.

```
                  ┌────────────────────┐
                  │ Transaction Source │
                  │ (merged_ieee_cis.csv)│
                  └────────┬───────────┘
                           │
           ┌───────────────▼───────────────┐
           │   transaction-producer (Python) │
           └───────────────┬───────────────┘
                           │ (Sends JSON messages)
           ┌───────────────▼───────────────┐
           │     Apache Kafka Topic        │
           │    (ieee_transactions)        │
           └───────────────┬───────────────┘
                           │
           ┌───────────────▼───────────────┐
           │    fraud-detector (Python)    │
           │ 1. Consumes from Kafka        │
           │ 2. Preprocesses data          │    ┌─────────────┐
           │ 3. Predicts using XGBoost Model ├────┤ Redis Cache │
           │ 4. Exposes Prometheus metrics │    └─────────────┘
           └───────────────┬───────────────┘
             ┌─────────────┴─────────────┐
             │ (Predictions & Raw Data)  │ (Metrics Scraped)
  ┌──────────▼──────────┐      ┌─────────▼────────┐
  │  PostgreSQL         │      │ Prometheus       │
  │ - ieee_transactions │      └─────────┬────────┘
  │ - ieee_predictions  │                │
  └──────────┬──────────┘                │
             │                           │
  ┌──────────▼───────────────────────────▼──────────┐
  │                 Grafana Dashboard                 │
  │ (Visualizes data from PostgreSQL & Prometheus)  │
  └─────────────────────────────────────────────────┘
```

## ✨ Key Features

-   **Real-Time Processing**: Leverages Apache Kafka for a high-throughput, low-latency event streaming backbone.
-   **Machine Learning**: Integrates a pre-trained XGBoost model for accurate fraud classification.
-   **Comprehensive Monitoring**: Features a Grafana dashboard for real-time visualization of transaction volumes, fraud rates, and system performance metrics.
-   **Persistent Storage**: Uses PostgreSQL to store all incoming transactions and their corresponding fraud predictions for analytics and auditing.
-   **Containerized & Scalable**: Fully containerized using Docker and Docker Compose for easy deployment and scalability.
-   **Metrics Exposure**: The fraud detector service exposes key performance indicators via a Prometheus-compatible endpoint.

## 🛠️ Tech Stack

-   **Stream Processing**: Apache Kafka
-   **Database**: PostgreSQL
-   **Caching**: Redis
-   **Monitoring**: Grafana, Prometheus
-   **Machine Learning**: Python, XGBoost, Pandas, Scikit-learn
-   **Containerization**: Docker, Docker Compose

## 🚀 Getting Started

### Prerequisites

-   Docker
-   Docker Compose
-   Git

### Installation & Launch

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/hariiiiiiiii/fraud-detection-system.git
    cd fraud-detection-system
    ```

2.  **Download the Dataset**
    This project uses the [IEEE-CIS Fraud Detection dataset](https://www.kaggle.com/c/ieee-fraud-detection/data). Due to its size, you need to download it manually.
    -   Download `train_transaction.csv`, `train_identity.csv`, `test_transaction.csv`, and `test_identity.csv`.
    -   Place them in the `./data/` directory.
    -   Merge them into a single file named `merged_ieee_cis.csv`.
        > *Note: A script for merging is not provided, but it can be done with a simple Pandas merge on `TransactionID`.*

3.  **Build and Run the Services**
    Start the entire pipeline using Docker Compose. This will build the Python service images and pull official images for the infrastructure components.
    ```bash
    docker-compose up --build -d
    ```
    The `-d` flag runs the services in detached mode. To view logs, you can use `docker-compose logs -f`.

4.  **Access the Services**
    Once the containers are running, you can access the various UI components:
    -   **Grafana Dashboard**: [http://localhost:3000](http://localhost:3000) (Login: `admin` / `admin123`)
    -   **Kafka UI**: [http://localhost:8080](http://localhost:8080)
    -   **Prometheus**: [http://localhost:9090](http://localhost:9090)
    -   **Detector Metrics**: [http://localhost:8000/metrics](http://localhost:8000/metrics)

5.  **Shutting Down**
    To stop and remove all running containers, run:
    ```bash
    docker-compose down
    ```

## 🧠 Machine Learning Model

The fraud detection logic is powered by an XGBoost Classifier trained on the IEEE-CIS dataset.

-   **Algorithm**: `XGBClassifier`
-   **Training**: The model was trained using a 5-fold stratified cross-validation approach to handle class imbalance. The training process and exploratory data analysis (EDA) can be found in the `notebooks/` directory.
-   **Feature Engineering**: The model uses features engineered from the raw transaction data, including:
    -   Log-transformation of `TransactionAmt`.
    -   Time-based features derived from `TransactionDT`.
    -   Frequency encoding for categorical features like `card1`, `addr1`, etc.
    -   Interaction features, such as the mean and standard deviation of transaction amounts per card.
-   **Performance**:
    -   **Average AUC-ROC**: ~0.966
    -   **Average F1-Score (for fraud class)**: ~0.74
    -   **Average Precision (for fraud class)**: ~0.81
    -   **Average Recall (for fraud class)**: ~0.69

The trained model (`xgb_model.pkl`) and the list of required features (`feature_cols.pkl`) are stored in the `models/` directory.

## 📊 Monitoring Dashboard

The Grafana dashboard, provisioned automatically, provides real-time insights into the pipeline's operation. It connects to both PostgreSQL (for business metrics) and Prometheus (for system metrics).

Key metrics displayed:
-   **Real-time Fraud Rate (%)**: The percentage of transactions flagged as fraudulent in the last hour.
-   **Transactions Per Hour**: Total number of transactions processed.
-   **Average Processing Time (ms)**: The average latency for a single transaction to be processed and a prediction to be made.
-   **High-Risk Transactions**: A count of transactions with a fraud probability greater than 0.5.
-   **Fraud Detection Over Time**: A time-series chart showing the volume of total vs. fraudulent transactions.

## 📁 Repository Structure

```
.
├── Dockerfile.detector         # Dockerfile for the fraud detection service
├── Dockerfile.producer         # Dockerfile for the transaction producer service
├── docker-compose.yml          # Defines and configures all services
├── fraud_detector.py           # Main script for the consumer service
├── requirements.txt            # Python dependencies
├── transaction_producer.py     # Script to simulate and send transactions to Kafka
├── data/                       # Directory for the input dataset (not included in repo)
├── grafana/
│   ├── dashboards/             # Grafana dashboard JSON definition
│   └── provisioning/           # Grafana provisioning for dashboards and datasources
├── models/
│   ├── xgb_model.pkl           # The pre-trained XGBoost model
│   └── feature_cols.pkl        # List of features used by the model
├── notebooks/
│   ├── eda.ipynb               # Exploratory Data Analysis notebook
│   └── main.ipynb              # Notebook for model training and evaluation
├── postgres/
│   └── init.sql                # Deprecated, see sql/init.sql
├── prometheus/
│   └── prometheus.yml          # Prometheus configuration file
└── sql/
    └── init.sql                # SQL script to initialize database indexes and views
```

## 💡 Future Improvements

-   [ ] **Real-time Model Retraining**: Implement a pipeline for periodically retraining the model on new data.
-   [ ] **Advanced Feature Engineering**: Incorporate a dedicated feature store for more complex, stateful features (e.g., historical user behavior).
-   [ ] **A/B Testing Framework**: Build a mechanism to deploy and test multiple models simultaneously.
-   [ ] **Enhanced Alerting**: Configure advanced alerting rules in Prometheus/Grafana for anomalies.
-   [ ] **Deployment on Kubernetes**: Create Helm charts for deploying the application on a Kubernetes cluster for better scalability and management.
