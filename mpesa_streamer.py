import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

# --- KENYAN CONTEXT DATA ---
LOCATIONS = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret", "Thika", "Machakos"]
TXN_TYPES = ["P2P_TRANSFER", "AGENT_WITHDRAWAL", "PAYBILL", "TILL_PAYMENT"]

# --- SETUP KAFKA PRODUCER ---
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("[YES] Connected to Kafka Broker.")
except Exception as e:
    print(f"[ERROR] Kafka connection failed: {e}")
    exit(1)

TOPIC = "live_transactions"

def generate_normal_transaction(sender_id):
    """Generates standard, everyday mobile money activity."""
    return {
        "txn_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sender_id": sender_id,
        "receiver_id": f"2547{random.randint(10000000, 99999999)}",
        "txn_type": random.choice(TXN_TYPES),
        "amount": round(random.uniform(50, 5000), 2),
        "location": random.choice(LOCATIONS),
        "device_id": f"DEV-{sender_id[-4:]}", # Consistent device for normal txns
        "is_synthetic_fraud": 0 # Ground truth for future XGBoost training
    }

def inject_account_takeover(sender_id):
    """Fraud Pattern 1: SIM Swap / Account Takeover
    High amount withdrawal from a completely new device in a different city at an odd hour."""
    return {
        "txn_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sender_id": sender_id,
        "receiver_id": f"AGT-{random.randint(1000, 9999)}",
        "txn_type": "AGENT_WITHDRAWAL",
        "amount": round(random.uniform(40000, 70000), 2), # Emptying account
        "location": "Mombasa", # Suddenly far away
        "device_id": f"DEV-{uuid.uuid4().hex[:6].upper()}", # Brand new device
        "is_synthetic_fraud": 1
    }

def inject_velocity_fraud(sender_id):
    """Fraud Pattern 2: Velocity / Ghost Agent
    Rapid fire transactions in a very short time window."""
    txns = []
    base_time = datetime.utcnow().timestamp()
    for i in range(4):
        txns.append({
            "txn_id": str(uuid.uuid4()),
            "timestamp": datetime.fromtimestamp(base_time + (i * 15)).isoformat() + "Z", # 15 seconds apart
            "sender_id": sender_id,
            "receiver_id": f"2547{random.randint(10000000, 99999999)}",
            "txn_type": "P2P_TRANSFER",
            "amount": round(random.uniform(25000, 35000), 2),
            "location": "Nairobi",
            "device_id": f"DEV-{sender_id[-4:]}",
            "is_synthetic_fraud": 1
        })
    return txns

if __name__ == "__main__":
    print("\n Starting M-Pesa Live Transaction Stream...")
    print("Injecting synthetic fraud typologies for ML training.\n")
    
    try:
        while True:
            # Pick a random user
            sender_id = f"2547{random.randint(10000000, 99999999)}"
            
            # 95% of the time, it's a normal transaction
            if random.random() < 0.95:
                txn = generate_normal_transaction(sender_id)
                producer.send(TOPIC, value=txn)
                print(f"[NORMAL]  | {txn['txn_type']} | KES {txn['amount']:,.2f} | {txn['location']}")
            
            # 5% of the time, inject a fraud typology
            else:
                fraud_type = random.choice(["takeover", "velocity"])
                if fraud_type == "takeover":
                    txn = inject_account_takeover(sender_id)
                    producer.send(TOPIC, value=txn)
                    print(f"[ FRAUD - ATO] | {txn['txn_type']} | KES {txn['amount']:,.2f} | NEW DEVICE: {txn['device_id']}")
                
                elif fraud_type == "velocity":
                    txns = inject_velocity_fraud(sender_id)
                    for t in txns:
                        producer.send(TOPIC, value=t)
                        print(f"[🚨 FRAUD - VELOCITY] | {t['txn_type']} | KES {t['amount']:,.2f} | RAPID FIRE")

            producer.flush()
            time.sleep(random.uniform(0.1, 0.8)) # High throughput simulation

    except KeyboardInterrupt:
        print("\n[!] Stopping stream...")
    finally:
        producer.close()