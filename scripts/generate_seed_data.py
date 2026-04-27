"""
generate_seed_data.py
─────────────────────
Generates realistic Nigerian neobank seed data and inserts it into
the postgres-source database.

Run locally:
    pip install faker psycopg2-binary python-dotenv
    python scripts/generate_seed_data.py

Or inside Docker:
    docker exec -it <airflow-scheduler-container> python /opt/airflow/scripts/generate_seed_data.py

Configuration via environment variables or defaults below.
"""

import os
import random
import uuid
import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

# ─── Config ───────────────────────────────────────────────────────────────────
DB_CONN = os.getenv(
    "SOURCE_DB_CONN_LOCAL",
    "postgresql://naija:naija123@localhost:5433/naijabank"
)
NUM_USERS        = int(os.getenv("SEED_USERS", "500"))
NUM_DAYS_HISTORY = int(os.getenv("SEED_DAYS", "90"))   # 3 months of history
RANDOM_SEED      = 42

fake = Faker("en_NG")   # Nigerian locale
random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)

# ─── Nigerian reference data ───────────────────────────────────────────────────
NIGERIAN_STATES = [
    "Lagos", "Abuja", "Kano", "Rivers", "Oyo", "Enugu", "Delta",
    "Anambra", "Kaduna", "Ogun", "Ondo", "Kwara", "Cross River",
    "Edo", "Benue", "Plateau", "Kogi", "Osun", "Ekiti", "Imo"
]

LAGOS_CITIES   = ["Victoria Island", "Ikeja", "Lekki", "Surulere", "Yaba",
                   "Ajah", "Ikorodu", "Apapa", "Festac", "Ojota"]
ABUJA_CITIES   = ["Maitama", "Wuse", "Garki", "Asokoro", "Jabi", "Gwarinpa"]
GENERAL_CITIES = ["GRA", "Trans-Ekulu", "Old GRA", "Rumuola", "Rumuokoro"]

CBN_BANKS = {
    "058": "Guaranty Trust Bank",
    "033": "United Bank for Africa",
    "057": "Zenith Bank",
    "044": "Access Bank",
    "070": "Fidelity Bank",
    "011": "First Bank of Nigeria",
    "032": "Union Bank",
    "035": "Wema Bank",
    "214": "First City Monument Bank",
    "215": "Unity Bank",
    "301": "Jaiz Bank",
    "327": "Palmpay",
    "304": "Stanbic IBTC",
    "000026": "Kuda Bank",
    "000033": "Moniepoint MFB",
}

TRANSACTION_TYPES = [
    ("transfer_out",  0.28, "debit"),
    ("transfer_in",   0.22, "credit"),
    ("airtime",       0.12, "debit"),
    ("data",          0.08, "debit"),
    ("bill_payment",  0.07, "debit"),
    ("pos_purchase",  0.10, "debit"),
    ("atm_withdrawal",0.06, "debit"),
    ("card_payment",  0.05, "debit"),
    ("reversal",      0.02, "credit"),
]
TXN_TYPES_NAMES   = [t[0] for t in TRANSACTION_TYPES]
TXN_TYPES_WEIGHTS = [t[1] for t in TRANSACTION_TYPES]
TXN_DIRECTIONS    = {t[0]: t[2] for t in TRANSACTION_TYPES}

CHANNELS = ["mobile_app", "mobile_app", "mobile_app", "ussd", "web", "pos", "atm"]

NARRATIONS = {
    "transfer_out":   ["Transfer to {name}", "Payment for {name}", "Sending to {name}",
                       "School fees - {name}", "Rent payment"],
    "transfer_in":    ["Transfer from {name}", "Refund from {name}", "Salary payment",
                       "Business income", "Gift from {name}"],
    "airtime":        ["MTN airtime top-up", "Glo airtime recharge", "Airtel airtime",
                       "9mobile airtime"],
    "data":           ["MTN data bundle", "Glo data plan", "Airtel data",
                       "9mobile data bundle"],
    "bill_payment":   ["IKEDC electricity bill", "EKEDC payment", "DSTV subscription",
                       "GOtv payment", "LAWMA levy"],
    "pos_purchase":   ["POS purchase at Shoprite", "POS - Spar", "POS - Game Store",
                       "Restaurant payment", "Fuel station"],
    "atm_withdrawal": ["ATM withdrawal", "Cash withdrawal"],
    "card_payment":   ["Online purchase", "Netflix subscription", "Bolt ride",
                       "Jumia order", "Paystack payment"],
    "reversal":       ["Reversal of failed transaction", "Chargeback credit"],
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def nigerian_phone() -> str:
    """Generate a realistic Nigerian mobile number."""
    prefixes = ["0801", "0802", "0803", "0805", "0806", "0807", "0808",
                "0809", "0810", "0811", "0812", "0813", "0814", "0815",
                "0816", "0817", "0818", "0901", "0902", "0903", "0904",
                "0905", "0906", "0907", "0908", "0909", "0911", "0912"]
    return random.choice(prefixes) + str(random.randint(1000000, 9999999))


def nuban_account_number() -> str:
    """Generate a 10-digit NUBAN-style account number."""
    return str(random.randint(1000000000, 9999999999))


def mask_bvn(bvn_digits: str) -> str:
    return bvn_digits[:3] + "****" + bvn_digits[-4:]


def referral_code(first_name: str, uid: str) -> str:
    h = hashlib.md5(uid.encode()).hexdigest()[:4].upper()
    return (first_name[:4].upper() + h)[:8]


def random_amount(txn_type: str) -> Decimal:
    """Return a realistic NGN amount for each transaction type."""
    ranges = {
        "transfer_out":   (500,    200_000),
        "transfer_in":    (1_000,  500_000),
        "airtime":        (100,    5_000),
        "data":           (300,    10_000),
        "bill_payment":   (1_000,  50_000),
        "pos_purchase":   (500,    80_000),
        "atm_withdrawal": (5_000,  100_000),
        "card_payment":   (1_000,  150_000),
        "reversal":       (500,    50_000),
    }
    lo, hi = ranges.get(txn_type, (500, 50_000))
    # Use a log-normal-ish distribution — most txns are small
    raw = random.expovariate(1 / ((lo + hi) / 4))
    clamped = min(max(raw, lo), hi)
    # Round to nearest 50 naira
    return Decimal(str(round(clamped / 50) * 50))


def narration_for(txn_type: str) -> str:
    templates = NARRATIONS.get(txn_type, ["Transaction"])
    t = random.choice(templates)
    return t.format(name=fake.name()) if "{name}" in t else t


def fee_for(amount: Decimal, txn_type: str) -> tuple[Decimal, Decimal]:
    """NIP fee tiers: ≤5k=10, ≤50k=25, >50k=50 NGN."""
    if txn_type not in ("transfer_out", "transfer_in"):
        return Decimal("0"), Decimal("0")
    if amount <= 5_000:
        fee = Decimal("10.75")
    elif amount <= 50_000:
        fee = Decimal("26.88")
    else:
        fee = Decimal("53.75")
    vat = (fee * Decimal("0.075")).quantize(Decimal("0.01"))
    return fee, vat


def random_status(txn_type: str) -> str:
    if txn_type == "reversal":
        return "successful"
    weights = [0.91, 0.06, 0.03]   # successful, failed, pending
    return random.choices(["successful", "failed", "pending"], weights=weights)[0]


def city_for_state(state: str) -> str:
    if state == "Lagos":
        return random.choice(LAGOS_CITIES)
    if state == "Abuja":
        return random.choice(ABUJA_CITIES)
    return random.choice(GENERAL_CITIES)


# ─── Generators ───────────────────────────────────────────────────────────────

def generate_users(n: int) -> list[dict]:
    users = []
    for _ in range(n):
        uid      = str(uuid.uuid4())
        fname    = fake.first_name()
        lname    = fake.last_name()
        state    = random.choice(NIGERIAN_STATES)
        kyc      = random.choices([1, 2, 3], weights=[0.4, 0.45, 0.15])[0]
        created  = datetime.now(timezone.utc) - timedelta(
            days=random.randint(NUM_DAYS_HISTORY, NUM_DAYS_HISTORY + 180)
        )
        users.append({
            "user_id":         uid,
            "first_name":      fname,
            "last_name":       lname,
            "email":           f"{fname.lower()}.{lname.lower()}{random.randint(1,99)}@{random.choice(['gmail.com','yahoo.com','outlook.com'])}",
            "phone_number":    nigerian_phone(),
            "bvn":             mask_bvn(str(random.randint(10000000000, 99999999999))),
            "state_of_origin": state,
            "city":            city_for_state(state),
            "kyc_level":       kyc,
            "kyc_verified_at": created + timedelta(hours=random.randint(1, 48)) if kyc > 1 else None,
            "is_active":       random.random() > 0.05,
            "referral_code":   referral_code(fname, uid),
            "referred_by":     None,   # filled in second pass
            "created_at":      created,
            "updated_at":      created,
        })
    # Second pass: assign ~30% of users a referrer
    user_ids = [u["user_id"] for u in users]
    for u in users:
        if random.random() < 0.30:
            u["referred_by"] = random.choice(user_ids)
    return users


def generate_accounts(users: list[dict]) -> list[dict]:
    accounts = []
    for user in users:
        # All users get one savings account
        accounts.append({
            "account_id":     str(uuid.uuid4()),
            "user_id":        user["user_id"],
            "account_number": nuban_account_number(),
            "account_type":   "savings",
            "currency":       "NGN",
            "balance":        Decimal(str(random.randint(0, 500_000))),
            "ledger_balance": Decimal(str(random.randint(0, 500_000))),
            "daily_limit":    Decimal("500000.00"),
            "is_frozen":      random.random() < 0.02,
            "freeze_reason":  "Suspicious activity" if random.random() < 0.02 else None,
            "opened_at":      user["created_at"],
            "closed_at":      None,
            "updated_at":     user["created_at"],
        })
        # 20% of users also have a wallet account
        if random.random() < 0.20:
            accounts.append({
                "account_id":     str(uuid.uuid4()),
                "user_id":        user["user_id"],
                "account_number": nuban_account_number(),
                "account_type":   "wallet",
                "currency":       "NGN",
                "balance":        Decimal(str(random.randint(0, 50_000))),
                "ledger_balance": Decimal(str(random.randint(0, 50_000))),
                "daily_limit":    Decimal("100000.00"),
                "is_frozen":      False,
                "freeze_reason":  None,
                "opened_at":      user["created_at"] + timedelta(days=random.randint(7, 60)),
                "closed_at":      None,
                "updated_at":     user["created_at"],
            })
    return accounts


def generate_transactions(accounts: list[dict]) -> list[dict]:
    now     = datetime.now(timezone.utc)
    start   = now - timedelta(days=NUM_DAYS_HISTORY)
    txns    = []
    references = set()

    for account in accounts:
        if account["is_frozen"]:
            continue
        # Avg 2-5 txns/day per account, distributed over history
        num_txns = random.randint(
            NUM_DAYS_HISTORY * 1,
            NUM_DAYS_HISTORY * 4
        )
        balance = float(account["balance"])

        for _ in range(num_txns):
            txn_type  = random.choices(TXN_TYPES_NAMES, weights=TXN_TYPES_WEIGHTS)[0]
            direction = TXN_DIRECTIONS[txn_type]
            amount    = random_amount(txn_type)
            fee, vat  = fee_for(amount, txn_type)
            status    = random_status(txn_type)
            channel   = random.choice(CHANNELS)
            created   = start + timedelta(
                seconds=random.randint(0, int((now - start).total_seconds()))
            )

            bal_before = Decimal(str(round(balance, 2)))
            if direction == "debit" and status == "successful":
                balance -= float(amount) + float(fee) + float(vat)
                balance  = max(balance, 0)
            elif direction == "credit" and status == "successful":
                balance += float(amount)
            bal_after = Decimal(str(round(balance, 2)))

            # Unique reference
            ref = f"NBK{created.strftime('%Y%m%d')}{uuid.uuid4().hex[:12].upper()}"
            while ref in references:
                ref = f"NBK{created.strftime('%Y%m%d')}{uuid.uuid4().hex[:12].upper()}"
            references.add(ref)

            # Interbank counterparty
            bank_code  = random.choice(list(CBN_BANKS.keys()))
            bank_name  = CBN_BANKS[bank_code]

            txns.append({
                "transaction_id":   str(uuid.uuid4()),
                "account_id":       account["account_id"],
                "transaction_type": txn_type,
                "amount":           amount,
                "currency":         "NGN",
                "direction":        direction,
                "balance_before":   bal_before,
                "balance_after":    bal_after,
                "status":           status,
                "channel":          channel,
                "narration":        narration_for(txn_type),
                "reference":        ref,
                "session_id":       uuid.uuid4().hex[:24].upper() if txn_type in ("transfer_out", "transfer_in") else None,
                "counterparty_bank": bank_name if txn_type in ("transfer_out", "transfer_in") else None,
                "counterparty_acct": nuban_account_number() if txn_type in ("transfer_out", "transfer_in") else None,
                "counterparty_name": fake.name() if txn_type in ("transfer_out", "transfer_in") else None,
                "fee_amount":        fee,
                "vat_amount":        vat,
                "device_id":         f"DEV-{uuid.uuid4().hex[:8].upper()}",
                "ip_address":        fake.ipv4(),
                "location_state":    account.get("state", "Lagos"),
                "created_at":        created,
                "updated_at":        created,
                "value_date":        created.date(),
            })

    return sorted(txns, key=lambda x: x["created_at"])


def generate_cards(accounts: list[dict], users: list[dict]) -> list[dict]:
    user_map = {u["user_id"]: u for u in users}
    cards = []
    for account in accounts:
        if account["account_type"] != "savings":
            continue
        if random.random() < 0.15:   # 15% haven't requested a card yet
            continue
        scheme   = random.choices(["verve", "mastercard", "visa"], weights=[0.6, 0.25, 0.15])[0]
        card_type = random.choices(["virtual", "physical"], weights=[0.7, 0.3])[0]
        pan_prefix = {"verve": "5061", "mastercard": "5234", "visa": "4751"}[scheme]
        masked_pan = f"{pan_prefix} **** **** {random.randint(1000, 9999)}"
        issued = account["opened_at"] + timedelta(days=random.randint(1, 30))
        exp_year  = issued.year + 4
        status   = random.choices(["active", "blocked"], weights=[0.92, 0.08])[0]
        cards.append({
            "card_id":      str(uuid.uuid4()),
            "account_id":   account["account_id"],
            "user_id":      account["user_id"],
            "card_type":    card_type,
            "card_scheme":  scheme,
            "masked_pan":   masked_pan,
            "expiry_month": issued.month,
            "expiry_year":  exp_year,
            "status":       status,
            "daily_limit":  Decimal("100000.00"),
            "issued_at":    issued,
            "blocked_at":   issued + timedelta(days=random.randint(10, 180)) if status == "blocked" else None,
            "block_reason": "Customer request" if status == "blocked" else None,
        })
    return cards


# ─── DB insertion ─────────────────────────────────────────────────────────────

def insert_batch(cur, table: str, rows: list[dict], chunk=500):
    if not rows:
        return
    cols = list(rows[0].keys())
    sql  = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s ON CONFLICT DO NOTHING"
    for i in range(0, len(rows), chunk):
        chunk_rows = rows[i:i+chunk]
        values = [tuple(r[c] for c in cols) for r in chunk_rows]
        execute_values(cur, sql, values)
    print(f"  ✓ Inserted {len(rows):,} rows into {table}")


def main():
    print("=" * 60)
    print("NaijaBank Seed Data Generator")
    print(f"  Users:       {NUM_USERS}")
    print(f"  Days history:{NUM_DAYS_HISTORY}")
    print("=" * 60)

    conn = psycopg2.connect(DB_CONN)
    conn.autocommit = False
    cur  = conn.cursor()

    try:
        print("\n[1/4] Generating users...")
        users = generate_users(NUM_USERS)
        insert_batch(cur, "users", users)

        print("\n[2/4] Generating accounts...")
        accounts = generate_accounts(users)
        insert_batch(cur, "accounts", accounts)

        print("\n[3/4] Generating transactions (this may take a moment)...")
        transactions = generate_transactions(accounts)
        insert_batch(cur, "transactions", transactions)

        print("\n[4/4] Generating cards...")
        cards = generate_cards(accounts, users)
        insert_batch(cur, "cards", cards)

        conn.commit()
        print("\n✅ Seed complete!")
        print(f"   {len(users):,} users")
        print(f"   {len(accounts):,} accounts")
        print(f"   {len(transactions):,} transactions")
        print(f"   {len(cards):,} cards")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
