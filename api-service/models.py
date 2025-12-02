from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    merchant: str
    location: str
    card_last4: str
    amount: float
    currency: str
    timestamp: str
    is_fraudulent: bool


class FraudAlert(BaseModel):
    transaction_id: str
    user_id: str
    merchant: str
    location: str
    card_last4: str
    amount: float
    currency: str
    timestamp: str
    is_fraudulent: bool


class StatsResponse(BaseModel):
    total_transactions: int
    total_fraud_alerts: int
    fraud_rate: float
    latest_transaction_time: Optional[str]
    latest_alert_time: Optional[str]

