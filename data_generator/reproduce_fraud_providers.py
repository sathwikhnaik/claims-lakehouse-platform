import random
import uuid
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_PROVIDERS = 40
providers = [str(uuid.uuid4()) for _ in range(NUM_PROVIDERS)]

fraud_providers = random.sample(providers, 2)
print("Fraud providers:", fraud_providers)