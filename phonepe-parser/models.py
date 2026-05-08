from pydantic import BaseModel, Field
from typing import List, Optional

class Transaction(BaseModel):
    date: str = Field(..., description="Transaction date in ISO format, with time when available (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)")
    entity: Optional[str] = Field(None, description="Extracted entity (person/merchant)")
    amount: float = Field(..., description="Transaction amount")
    type: str = Field(..., description="DEBIT or CREDIT")
    category: str = Field(..., description="Derived category")
    raw_details: str = Field(..., description="Raw transaction text")
    utr_number: Optional[str] = Field(None, description="Extracted UTR Number / Reference ID")

class Summary(BaseModel):
    total_transactions: int
    total_debit: float
    total_credit: float

class ParseResponse(BaseModel):
    transactions: List[Transaction]
    summary: Summary
