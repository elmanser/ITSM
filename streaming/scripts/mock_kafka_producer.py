import os
import json
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "itsm.tickets.raw"

def generate_mock_tickets(n=500):
    tickets = []
    priorities = [1, 2, 3, 4, 5]
    statuses = [1, 2, 3, 4, 5, 6]
    categories = ["network", "hardware", "software", "security", "access"]
    
    now = datetime.now(timezone.utc)
    
    for i in range(1, n + 1):
        created = now - timedelta(days=random.randint(0, 180), hours=random.randint(0, 23))
        
        status = random.choice(statuses)
        if status in [5, 6]: # solved or closed
            solved = created + timedelta(hours=random.randint(1, 48))
            closed = solved + timedelta(days=random.randint(1, 3))
        else:
            solved = None
            closed = None

        ticket = {
            "id": i,
            "name": f"Mock Ticket #{i}",
            "content": f"This is an auto-generated mock ticket for testing the pipeline.",
            "priority": random.choice(priorities),
            "status": status,
            "urgency": random.randint(1, 5),
            "impact": random.randint(1, 5),
            "date_creation": created.strftime("%Y-%m-%d %H:%M:%S"),
            "solvedate": solved.strftime("%Y-%m-%d %H:%M:%S") if solved else None,
            "closedate": closed.strftime("%Y-%m-%d %H:%M:%S") if closed else None,
            "itilcategories_id": random.choice(categories),
            "_users_id_requester": f"User_{random.randint(1, 100)}",
            "_groups_id_assign": f"Group_{random.randint(1, 10)}"
        }
        tickets.append(ticket)
    
    # Sort by creation date
    tickets.sort(key=lambda x: x["date_creation"])
    return tickets

def main():
    print(f"Connecting to Kafka at {KAFKA_BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    
    tickets = generate_mock_tickets(1500)
    print(f"Generated {len(tickets)} mock tickets. Publishing to '{KAFKA_TOPIC}'...")
    
    for t in tickets:
        msg = {
            "event_type": "ticket_ingested", 
            "source": "mock_generator",
            "ingested_at": datetime.now(timezone.utc).isoformat(), 
            "data": t
        }
        producer.send(KAFKA_TOPIC, key=str(t["id"]), value=msg)
    
    producer.flush()
    producer.close()
    print("Successfully published all mock tickets to Kafka!")

if __name__ == "__main__":
    main()
