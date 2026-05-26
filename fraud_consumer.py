import json
import warnings
import pandas as pd
import xgboost as xgb
import psycopg2
from pymongo import MongoClient
from kafka import KafkaConsumer
from datetime import datetime

# Suppress warnings for clean terminal output
warnings.filterwarnings("ignore")

# --- DATABASE CONNECTIONS ---
def get_postgres_conn():
    return psycopg2.connect(
        host="localhost", port=5433, database="mobile_money_profiles",
        user="risk_admin", password="RiskPassword2026"
    )

def get_mongo_collection():
    client = MongoClient("mongodb://mongo_risk_admin:MongoPassword2026@localhost:27017/?authSource=admin")
    return client["mpesa_audit_lake"]["scored_transactions"]

# --- INFRASTRUCTURE INITIALIZATION ---
def init_postgres():
    """Ensure the user profiles table and alerts table exist."""
    conn = get_postgres_conn()
    cur = conn.cursor()
    
    # 1. Profile Table (Feature Store)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_profiles (
            sender_id VARCHAR(50) PRIMARY KEY,
            last_device_id VARCHAR(50),
            last_location VARCHAR(50)
        );
    """)
    
    # 2. Alerts Table (For Grafana)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_transactions (
            txn_id VARCHAR(100) PRIMARY KEY,
            created_at TIMESTAMP,
            sender_id VARCHAR(50),
            txn_type VARCHAR(50),
            amount NUMERIC(10, 2),
            risk_score NUMERIC(5, 2),
            is_fraud BOOLEAN
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("[✓] PostgreSQL Feature Store & Grafana Alert Tables Initialized.")

# --- THE ML MODEL ---
def train_mock_model():
    """Trains a dummy XGBoost model so the engine works out-of-the-box."""
    print("[⚙️] Training localized XGBoost model on synthetic history...")
    # Features: [amount, is_new_device (0/1), is_new_location (0/1)]
    import numpy as np
    
    X_train = np.random.rand(1000, 3)
    X_train[:, 0] *= 50000  # Amount scale
    X_train[:, 1] = np.round(X_train[:, 1]) # Device match (1 = new device)
    X_train[:, 2] = np.round(X_train[:, 2]) # Location match (1 = new location)
    
    # Logic: High amount AND new device AND new location strongly correlates to fraud (1)
    y_train = ((X_train[:, 0] > 20000) & (X_train[:, 1] == 1) & (X_train[:, 2] == 1)).astype(int)
    
    model = xgb.XGBClassifier(eval_metric='logloss', max_depth=3)
    model.fit(X_train, y_train)
    print("[✓] XGBoost Model Ready.\n")
    return model

# --- MAIN INFERENCE PIPELINE ---
def start_consumer():
    init_postgres()
    model = train_mock_model()
    mongo_col = get_mongo_collection()
    pg_conn = get_postgres_conn()
    
    consumer = KafkaConsumer(
        'live_transactions',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("🚀 ML Inference Engine Active. Listening for transactions...")
    print("-" * 65)

    for message in consumer:
        txn = message.value
        sender_id = txn["sender_id"]
        
        # 1. Feature Retrieval (PostgreSQL)
        with pg_conn.cursor() as cur:
            cur.execute("SELECT last_device_id, last_location FROM user_profiles WHERE sender_id = %s", (sender_id,))
            profile = cur.fetchone()
            
            is_new_device = 0
            is_new_location = 0
            
            if profile:
                # Compare current transaction to historical profile
                if profile[0] != txn["device_id"]: is_new_device = 1
                if profile[1] != txn["location"]: is_new_location = 1
                
                # Update profile with latest info
                cur.execute("UPDATE user_profiles SET last_device_id = %s, last_location = %s WHERE sender_id = %s",
                            (txn["device_id"], txn["location"], sender_id))
            else:
                # First time seeing this user
                cur.execute("INSERT INTO user_profiles (sender_id, last_device_id, last_location) VALUES (%s, %s, %s)",
                            (sender_id, txn["device_id"], txn["location"]))
            pg_conn.commit()

        # 2. Real-Time Inference (XGBoost)
        features = pd.DataFrame([[txn["amount"], is_new_device, is_new_location]], 
                                columns=['amount', 'is_new_device', 'is_new_location'])
        
        fraud_prob = float(model.predict_proba(features)[0][1])
        risk_score = round(fraud_prob * 100, 2)
        
        # 3. Enrichment
        txn["risk_score"] = risk_score
        txn["processed_at"] = datetime.utcnow().isoformat() + "Z"
        
        # 4. Storage (MongoDB - The Audit Lake)
        mongo_col.insert_one(txn.copy())
        
        # 4b. Storage (PostgreSQL - The Grafana Alert Store)
        with pg_conn.cursor() as cur:
            pg_timestamp = txn["timestamp"].replace("T", " ")[:19]
            is_fraud = bool(risk_score > 80)
            
            cur.execute("""
                INSERT INTO processed_transactions 
                (txn_id, created_at, sender_id, txn_type, amount, risk_score, is_fraud)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (txn["txn_id"], pg_timestamp, sender_id, txn["txn_type"], txn["amount"], risk_score, is_fraud))
            pg_conn.commit()
        
        # 5. Alerting output
        if risk_score > 80:
            print(f"[ BLOCK]  Score: {risk_score:5.1f}% | {txn['txn_type']:<16} KES {txn['amount']:8,.2f} | Reason: New Device/Location")
        elif risk_score > 40:
            print(f"[ REVIEW] Score: {risk_score:5.1f}% | {txn['txn_type']:<16} KES {txn['amount']:8,.2f} | Action: Send OTP")
        else:
            print(f"[ PASS]   Score: {risk_score:5.1f}% | {txn['txn_type']:<16} KES {txn['amount']:8,.2f}")

if __name__ == "__main__":
    start_consumer()