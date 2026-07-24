# data_generator/generate_claims.py
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import json

fake = Faker()
Faker.seed(42)
random.seed(42)

PROCEDURE_CODES = {
    "99213": ("Office visit, established patient", 100, 250),
    "99214": ("Office visit, moderate complexity", 150, 350),
    "70450": ("CT scan, head", 800, 1500),
    "80053": ("Comprehensive metabolic panel", 30, 90),
    "36415": ("Blood draw", 10, 30),
    "99283": ("ER visit, moderate severity", 400, 900),
}

NUM_PROVIDERS = 40
NUM_PATIENTS = 2000
NUM_CLAIMS = 50_000

providers = [str(uuid.uuid4()) for _ in range(NUM_PROVIDERS)]
patients = [str(uuid.uuid4()) for _ in range(NUM_PATIENTS)]

# pick a couple of providers to inject an anomalous billing spike for later fraud detection
fraud_providers = random.sample(providers, 2)

def make_claim(claim_time):
    provider_id = random.choice(providers)
    code, (desc, lo, hi) = random.choice(list(PROCEDURE_CODES.items()))
    billed = round(random.uniform(lo, hi), 2)

    # inject anomaly: fraud providers bill 4-6x normal on ~15% of their claims
    if provider_id in fraud_providers and random.random() < 0.15:
        billed = round(billed * random.uniform(4, 6), 2)

    return {
        "claim_id": str(uuid.uuid4()),
        "patient_id": random.choice(patients),
        "provider_id": provider_id,
        "procedure_code": code,
        "procedure_desc": desc,
        "billed_amount": billed,
        "submitted_at": claim_time.isoformat(),
    }

def generate(path="sample_data/claims_submitted.jsonl"):
    start = datetime.utcnow() - timedelta(days=90)
    with open(path, "w") as f:
        for i in range(NUM_CLAIMS):
            claim_time = start + timedelta(seconds=random.randint(0, 90 * 24 * 3600))
            f.write(json.dumps(make_claim(claim_time)) + "\n")
    print(f"Wrote {NUM_CLAIMS} claims to {path}")
    print(f"Injected fraud pattern into providers: {fraud_providers}")

if __name__ == "__main__":
    generate()