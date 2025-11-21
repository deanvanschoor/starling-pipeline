from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
 
'''Pydantic models for webhook payload validation'''

class Amount(BaseModel):
    currency: str
    minorUnits: int
    
class RoundUp(BaseModel):
    goalCategoryUid: str
    amount: Amount

class FeedItemContent(BaseModel):
    feedItemUid: str
    categoryUid: str
    accountUid: str
    amount: Amount
    sourceAmount: Amount
    direction: str
    updatedAt: datetime
    transactionTime: datetime
    settlementTime: datetime
    source: str
    sourceSubType: Optional[str] = None
    status: str
    transactingApplicationUserUid: str
    counterPartyType: str
    counterPartyUid: str
    counterPartyName: str
    counterPartySubEntityUid: Optional[str] = None
    counterPartySubEntityName: Optional[str] = None
    counterPartySubEntityIdentifier: Optional[str] = None
    counterPartySubEntitySubIdentifier: Optional[str] = None
    exchangeRate: Optional[float] = None
    totalFeeAmount: Optional[Amount] = None
    reference: Optional[str] = None
    country: Optional[str] = None
    spendingCategory: Optional[str] = None
    userNote: Optional[str] = None
    roundUp: Optional[RoundUp] = None
    hasAttachment: bool
    receiptPresent: bool
    feedItemFailureReason: Optional[str] = None
    sourceUid: Optional[str] = None

class WebhookPayload(BaseModel):
    webhookEventUid: str
    eventTimestamp: datetime
    accountHolderUid: str
    content: FeedItemContent
    

 
if __name__ == "__main__":
    pass