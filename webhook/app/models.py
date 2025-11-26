from pydantic import BaseModel, Field , ConfigDict
from datetime import datetime
from typing import Optional
 
'''Pydantic models for webhook payload validation'''

class Amount(BaseModel):
    currency: str
    minorUnits: int
    
    model_config = ConfigDict(extra='allow') 
    
class RoundUp(BaseModel):
    goalCategoryUid: str
    amount: Amount
    
    model_config = ConfigDict(extra='allow') 

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
    source: Optional[str] = None
    sourceSubType: Optional[str] = None
    status: Optional[str] = None
    transactingApplicationUserUid: Optional[str] = None
    counterPartyType: Optional[str] = None
    counterPartyUid: Optional[str] = None
    counterPartyName: Optional[str] = None
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
    
    model_config = ConfigDict(extra='allow')

class WebhookPayload(BaseModel):
    webhookEventUid: str
    eventTimestamp: datetime
    accountHolderUid: str
    content: FeedItemContent
    
    model_config = ConfigDict(extra='allow') 
    

 
if __name__ == "__main__":
    pass