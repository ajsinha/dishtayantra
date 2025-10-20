import json
import random
from datetime import datetime, timedelta
import uuid

# Configuration
currencies = ["USD", "EUR", "JPY", "GBP"]
counterparties = ["JPMorgan", "Goldman Sachs", "Citibank", "Hedge Fund Alpha"]
venues = ["NYSE", "NASDAQ", "CME", "CBOE", "OTC", "LCH"]
clearing_houses = ["CME", "LCH", "ICE", "None"]
buy_sell = ["Buy", "Sell"]
trade_statuses = ["Executed", "Pending", "Cleared"]
tickers = {
    "Equity": [("AAPL", "US0378331005"), ("TSLA", "US88160R1014"), ("MSFT", "US5949181045")],
    "Fixed Income": [("US10Y", "912810SH1"), ("CORP_ABC", "123456AB9"), ("TNOTE_5Y", "912828ZV7")],
    "Commodity": [("CL", "CME_CL"), ("GC", "CME_GC"), ("SI", "CME_SI")],
    "FX": ["EUR/USD", "USD/JPY", "GBP/USD"]
}
floating_rate_indices = ["SOFR", "EURIBOR", "LIBOR"]
reference_entities = ["ABC Corp", "XYZ Inc", "US Treasury"]
coupon_frequencies = ["Semiannual", "Annual"]

def random_date(start, end):
    delta = end - start
    random_days = random.randrange(delta.days)
    return start + timedelta(days=random_days)

def random_time():
    return f"{random.randint(0, 23):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}"

def generate_cash_flows(start_date, maturity_date, frequency, amount):
    cash_flows = []
    current_date = start_date
    while current_date < maturity_date:
        if frequency == "Semiannual":
            current_date += timedelta(days=180)
        elif frequency == "Annual":
            current_date += timedelta(days=360)
        elif frequency == "Quarterly":
            current_date += timedelta(days=90)
        if current_date < maturity_date:
            cash_flows.append({"date": current_date.strftime("%Y-%m-%d"), "amount": round(amount, 2)})
    return cash_flows

def generate_amortization_schedule(start_date, maturity_date, face_value, frequency):
    schedule = []
    remaining_principal = face_value
    periods = int((maturity_date - start_date).days / (180 if frequency == "Semiannual" else 360))
    repayment = face_value / periods if periods > 0 else face_value
    current_date = start_date
    for _ in range(periods):
        if frequency == "Semiannual":
            current_date += timedelta(days=180)
        else:
            current_date += timedelta(days=360)
        if current_date < maturity_date:
            schedule.append({
                "date": current_date.strftime("%Y-%m-%d"),
                "principal_payment": round(repayment, 2),
                "remaining_principal": round(remaining_principal - repayment, 2)
            })
            remaining_principal -= repayment
    return schedule

def generate_equity_trade():
    ticker, isin = random.choice(tickers["Equity"])
    quantity = random.randint(1, 10000)
    price = round(random.uniform(50, 500), 2)
    trade_date = random_date(datetime(2024, 1, 1), datetime(2025, 12, 31))
    notional = quantity * price
    trade_fee = round(notional * random.uniform(0.0001, 0.001), 2)
    cash_flow = [{"date": (trade_date + timedelta(days=2)).strftime("%Y-%m-%d"), "amount": round(notional * (-1 if buy_sell[0] == "Buy" else 1), 2)}]
    return {
        "trade_id": str(uuid.uuid4()),
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "trade_time": random_time(),
        "settlement_date": (trade_date + timedelta(days=2)).strftime("%Y-%m-%d"),
        "product_type": "Equity",
        "subproduct": "Stock",
        "asset_class": "Equity",
        "ticker": ticker,
        "isin": isin,
        "notional_amount": round(notional, 2),
        "currency": random.choice(currencies),
        "price": price,
        "quantity": quantity,
        "cash_flows": cash_flow,
        "trade_fee": trade_fee,
        "counterparty": random.choice(counterparties),
        "venue": random.choice(["NYSE", "NASDAQ"]),
        "buy_sell": random.choice(buy_sell),
        "trade_status": random.choice(trade_statuses)
    }

def generate_fixed_income_trade(subproduct):
    ticker, cusip = random.choice(tickers["Fixed Income"])
    face_value = random.randint(100000, 10000000)
    price = round(random.uniform(95, 105), 2)
    coupon_rate = round(random.uniform(0.005, 0.05), 4)
    trade_date = random_date(datetime(2024, 1, 1), datetime(2025, 12, 31))
    maturity_date = random_date(datetime(2026, 1, 1), datetime(2040, 12, 31))
    frequency = random.choice(coupon_frequencies)
    notional = face_value * price / 100
    coupon_amount = face_value * coupon_rate / (2 if frequency == "Semiannual" else 1)
    cash_flows = generate_cash_flows(trade_date, maturity_date, frequency, coupon_amount)
    cash_flows.append({"date": maturity_date.strftime("%Y-%m-%d"), "amount": face_value})  # Principal repayment
    amortization = generate_amortization_schedule(trade_date, maturity_date, face_value, frequency) if subproduct == "Corporate Bond" and random.random() < 0.3 else []
    return {
        "trade_id": str(uuid.uuid4()),
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "trade_time": random_time(),
        "settlement_date": (trade_date + timedelta(days=3)).strftime("%Y-%m-%d"),
        "product_type": "Fixed Income",
        "subproduct": subproduct,
        "asset_class": "Fixed Income",
        "ticker": ticker,
        "cusip": cusip,
        "notional_amount": round(notional, 2),
        "currency": random.choice(currencies),
        "price": price,
        "face_value": face_value,
        "coupon_rate": coupon_rate,
        "coupon_frequency": frequency,
        "maturity_date": maturity_date.strftime("%Y-%m-%d"),
        "cash_flows": cash_flows,
        "amortization_schedule": amortization,
        "counterparty": random.choice(counterparties),
        "venue": "OTC",
        "buy_sell": random.choice(buy_sell),
        "trade_status": random.choice(trade_statuses)
    }

def generate_option_trade(subproduct):
    asset_class = random.choice(["Equity", "Commodity"])
    ticker, isin = random.choice(tickers[asset_class])
    quantity = random.randint(1, 500)
    premium = round(random.uniform(1, 50), 2)
    underlying_price = round(random.uniform(50, 500), 2)
    trade_date = random_date(datetime(2024, 1, 1), datetime(2025, 12, 31))
    maturity_date = random_date(datetime(2025, 1, 1), datetime(2027, 12, 31))
    cash_flow = [{"date": trade_date.strftime("%Y-%m-%d"), "amount": round(quantity * premium * 100 * (-1 if buy_sell[0] == "Buy" else 1), 2)}]
    return {
        "trade_id": str(uuid.uuid4()),
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "trade_time": random_time(),
        "settlement_date": (trade_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        "product_type": "Derivative",
        "subproduct": subproduct,
        "asset_class": asset_class,
        "ticker": ticker,
        "isin": isin,
        "notional_amount": round(quantity * underlying_price * 100, 2),
        "currency": random.choice(currencies),
        "premium": premium,
        "strike_price": round(random.uniform(50, 500), 2),
        "quantity": quantity,
        "maturity_date": maturity_date.strftime("%Y-%m-%d"),
        "option_type": subproduct,
        "underlying_asset": ticker,
        "cash_flows": cash_flow,
        "counterparty": random.choice(counterparties),
        "venue": random.choice(["CME", "CBOE"]),
        "buy_sell": random.choice(buy_sell),
        "trade_status": random.choice(trade_statuses),
        "clearing_house": random.choice(clearing_houses)
    }

def generate_futures_trade():
    ticker, isin = random.choice(tickers["Commodity"])
    quantity = random.randint(1, 100)
    price = round(random.uniform(50, 2000), 2)
    trade_date = random_date(datetime(2024, 1, 1), datetime(2025, 12, 31))
    contract_size = 1000 if ticker == "CL" else 100
    margin = round(quantity * price * contract_size * random.uniform(0.05, 0.15), 2)
    cash_flow = [{"date": trade_date.strftime("%Y-%m-%d"), "amount": round(margin * (-1 if buy_sell[0] == "Buy" else 1), 2)}]
    return {
        "trade_id": str(uuid.uuid4()),
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "trade_time": random_time(),
        "settlement_date": (trade_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        "product_type": "Derivative",
        "subproduct": "Futures",
        "asset_class": "Commodity",
        "ticker": ticker,
        "isin": isin,
        "notional_amount": round(quantity * price * contract_size, 2),
        "currency": random.choice(currencies),
        "quantity": quantity,
        "contract_size": contract_size,
        "margin": margin,
        "delivery_date": random_date(datetime(2025, 1, 1), datetime(2026, 12, 31)).strftime("%Y-%m-%d"),
        "cash_flows": cash_flow,
        "counterparty": random.choice(counterparties),
        "venue": "CME",
        "buy_sell": random.choice(buy_sell),
        "trade_status": random.choice(trade_statuses),
        "clearing_house": random.choice(clearing_houses)
    }

def generate_irs_trade():
    trade_date = random_date(datetime(2024, 1, 1), datetime(2025, 12, 31))
    maturity_date = random_date(datetime(2026, 1, 1), datetime(2055, 12, 31))
    notional = round(random.uniform(1000000, 100000000), 2)
    fixed_rate = round(random.uniform(0.01, 0.05), 4)
    frequency = "Quarterly"
    fixed_payment = notional * fixed_rate / 4
    cash_flows = generate_cash_flows(trade_date, maturity_date, frequency, fixed_payment)
    return {
        "trade_id": str(uuid.uuid4()),
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "trade_time": random_time(),
        "settlement_date": (trade_date + timedelta(days=2)).strftime("%Y-%m-%d"),
        "product_type": "Derivative",
        "subproduct": "Interest Rate Swap",
        "asset_class": "Fixed Income",
        "notional_amount": notional,
        "currency": random.choice(currencies),
        "fixed_rate": fixed_rate,
        "floating_rate_index": random.choice(floating_rate_indices),
        "coupon_frequency": frequency,
        "maturity_date": maturity_date.strftime("%Y-%m-%d"),
        "cash_flows": cash_flows,
        "counterparty": random.choice(counterparties),
        "venue": "OTC",
        "buy_sell": random.choice(buy_sell),
        "trade_status": random.choice(trade_statuses),
        "clearing_house": random.choice(clearing_houses)
    }

def generate_cds_trade():
    trade_date = random_date(datetime(2024, 1, 1), datetime(2025, 12, 31))
    maturity_date = random_date(datetime(2026, 1, 1), datetime(2035, 12, 31))
    notional = round(random.uniform(1000000, 50000000), 2)
    spread = random.randint(50, 500)  # Basis points
    premium = notional * spread / 10000 / 4  # Quarterly premium
    cash_flows = generate_cash_flows(trade_date, maturity_date, "Quarterly", premium * (-1 if buy_sell[0] == "Buy" else 1))
    return {
        "trade_id": str(uuid.uuid4()),
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "trade_time": random_time(),
        "settlement_date": (trade_date + timedelta(days=2)).strftime("%Y-%m-%d"),
        "product_type": "Derivative",
        "subproduct": "Credit Default Swap",
        "asset_class": "Credit",
        "reference_entity": random.choice(reference_entities),
        "notional_amount": notional,
        "currency": random.choice(currencies),
        "premium": spread,  # In basis points
        "maturity_date": maturity_date.strftime("%Y-%m-%d"),
        "cash_flows": cash_flows,
        "counterparty": random.choice(counterparties),
        "venue": "OTC",
        "buy_sell": random.choice(buy_sell),
        "trade_status": random.choice(trade_statuses),
        "clearing_house": random.choice(clearing_houses)
    }

def generate_fx_spot_trade():
    ticker = random.choice(tickers["FX"])
    trade_date = random_date(datetime(2024, 1, 1), datetime(2025, 12, 31))
    notional = round(random.uniform(100000, 10000000), 2)
    spot_rate = round(random.uniform(0.8, 1.5), 4)
    cash_flow = [{"date": (trade_date + timedelta(days=2)).strftime("%Y-%m-%d"), "amount": round(notional * spot_rate * (-1 if buy_sell[0] == "Buy" else 1), 2)}]
    return {
        "trade_id": str(uuid.uuid4()),
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "trade_time": random_time(),
        "settlement_date": (trade_date + timedelta(days=2)).strftime("%Y-%m-%d"),
        "product_type": "Cash",
        "subproduct": "FX Spot",
        "asset_class": "FX",
        "ticker": ticker,
        "notional_amount": notional,
        "currency": ticker.split("/")[1],
        "spot_rate": spot_rate,
        "cash_flows": cash_flow,
        "counterparty": random.choice(counterparties),
        "venue": "OTC",
        "buy_sell": random.choice(buy_sell),
        "trade_status": random.choice(trade_statuses)
    }

# Generate 1000 trades
trades = []
product_counts = {
    "Equity": 200,
    "Government Bond": 150,
    "Corporate Bond": 150,
    "Treasury Note": 100,
    "Call Option": 100,
    "Put Option": 100,
    "Futures": 100,
    "Interest Rate Swap": 100,
    "Credit Default Swap": 50,
    "FX Spot": 100
}

for subproduct, count in product_counts.items():
    for _ in range(count):
        if subproduct == "Stock":
            trades.append(generate_equity_trade())
        elif subproduct in ["Government Bond", "Corporate Bond", "Treasury Note"]:
            trades.append(generate_fixed_income_trade(subproduct))
        elif subproduct in ["Call Option", "Put Option"]:
            trades.append(generate_option_trade(subproduct))
        elif subproduct == "Futures":
            trades.append(generate_futures_trade())
        elif subproduct == "Interest Rate Swap":
            trades.append(generate_irs_trade())
        elif subproduct == "Credit Default Swap":
            trades.append(generate_cds_trade())
        elif subproduct == "FX Spot":
            trades.append(generate_fx_spot_trade())

# Shuffle trades
random.shuffle(trades)

# Save to JSON file
with open("/home/ashutosh/PycharmProjects/dagcomputeserver_v2/config/example/data/sample_trades.json", "w") as f:
    json.dump(trades, f, indent=4)

print("Generated 1000 trades and saved to /home/ashutosh/PycharmProjects/dagcomputeserver_v2/config/example/data/sample_trades.json")