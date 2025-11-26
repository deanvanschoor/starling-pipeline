"""Simple unit tests for Pydantic models"""
from datetime import datetime, timezone
from app.models import Amount, WebhookPayload, FeedItemContent

def test_amount_model():
    """Test Amount model with valid data"""
    amount = Amount(currency="GBP", minorUnits=1000)
    assert amount.currency == "GBP"
    assert amount.minorUnits == 1000

def test_valid_webhook_payload():
    """Test WebhookPayload with minimal valid data"""
    payload_data = {
        "webhookEventUid": "test-123",
        "webhookType": "FEED_ITEM",
        "eventTimestamp": "2025-11-26T12:00:00Z",
        "accountHolderUid": "holder-123",
        "content": {
            "feedItemUid": "feed-123",
            "categoryUid": "cat-123",
            "accountUid": "acc-123",
            "amount": {"currency": "GBP", "minorUnits": 1000},
            "sourceAmount": {"currency": "GBP", "minorUnits": 1000},
            "direction": "OUT",
            "updatedAt": "2025-11-26T12:00:00Z",
            "transactionTime": "2025-11-26T12:00:00Z",
            "settlementTime": "2025-11-26T12:00:00Z",
            "source": "MASTER_CARD",
            "status": "SETTLED",
            "transactingApplicationUserUid": "user-123",
            "counterPartyType": "MERCHANT",
            "counterPartyUid": "merchant-123",
            "counterPartyName": "Test Store",
            "hasAttachment": False,
            "receiptPresent": False
        }
    }
    
    payload = WebhookPayload(**payload_data)
    assert payload.webhookEventUid == "test-123"
    assert payload.content.feedItemUid == "feed-123"
    assert payload.content.amount.minorUnits == 1000

def test_optional_fields_allowed():
    """Test that optional fields can be omitted"""
    payload_data = {
        "webhookEventUid": "test-123",
        "webhookType": "FEED_ITEM",
        "eventTimestamp": "2025-11-26T12:00:00Z",
        "accountHolderUid": "holder-123",
        "content": {
            "feedItemUid": "feed-123",
            "categoryUid": "cat-123",
            "accountUid": "acc-123",
            "amount": {"currency": "GBP", "minorUnits": 1000},
            "sourceAmount": {"currency": "GBP", "minorUnits": 1000},
            "direction": "OUT",
            "updatedAt": "2025-11-26T12:00:00Z",
            "transactionTime": "2025-11-26T12:00:00Z",
            "settlementTime": "2025-11-26T12:00:00Z",
            "source": "MASTER_CARD",
            "status": "SETTLED",
            "transactingApplicationUserUid": "user-123",
            "counterPartyType": "MERCHANT",
            "counterPartyUid": "merchant-123",
            "counterPartyName": "Test Store",
            "hasAttachment": False,
            "receiptPresent": False
            # Note: No optional fields like userNote, reference, etc.
        }
    }
    
    payload = WebhookPayload(**payload_data)
    assert payload.content.userNote is None
    assert payload.content.reference is None

def test_extra_fields_ignored():
    """Test that extra unexpected fields are ignored"""
    payload_data = {
        "webhookEventUid": "test-123",
        "webhookType": "FEED_ITEM",
        "eventTimestamp": "2025-11-26T12:00:00Z",
        "accountHolderUid": "holder-123",
        "unexpectedField": "should be ignored",  # Extra field
        "content": {
            "feedItemUid": "feed-123",
            "categoryUid": "cat-123",
            "accountUid": "acc-123",
            "amount": {"currency": "GBP", "minorUnits": 1000},
            "sourceAmount": {"currency": "GBP", "minorUnits": 1000},
            "direction": "OUT",
            "updatedAt": "2025-11-26T12:00:00Z",
            "transactionTime": "2025-11-26T12:00:00Z",
            "settlementTime": "2025-11-26T12:00:00Z",
            "source": "MASTER_CARD",
            "status": "SETTLED",
            "transactingApplicationUserUid": "user-123",
            "counterPartyType": "MERCHANT",
            "counterPartyUid": "merchant-123",
            "counterPartyName": "Test Store",
            "hasAttachment": False,
            "receiptPresent": False,
            "newStarlingField": "ignored"  # Extra field in content
        }
    }
    
    # Should not raise an error
    payload = WebhookPayload(**payload_data)
    assert payload.webhookEventUid == "test-123"