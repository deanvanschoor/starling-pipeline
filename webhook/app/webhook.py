import logging
from flask import Flask, request, jsonify
from pydantic import ValidationError
from datetime import datetime
from werkzeug.exceptions import Unauthorized

from app.models import WebhookPayload
from app.constants import get_md_connection, ACCOUNT_UUID
from app.trigger import trigger_pipeline
from app.utils.logging_config import setup_logging

app = Flask(__name__)
log = logging.getLogger(__name__)

def insert_webhook_data(payload: WebhookPayload) -> bool:
    """Insert webhook payload into MotherDuck table"""
    try:
        conn = get_md_connection()
        
        insert_query = """      
        INSERT OR REPLACE INTO lnd.transactions_webhook (
            feedItemUid,
            categoryUid,
            accountUid,
            amount_currency,
            amount_minorUnits,
            sourceAmount_currency,
            sourceAmount_minorUnits,
            direction,
            updatedAt,
            transactionTime,
            settlementTime,
            source,
            status,
            transactingApplicationUserUid,
            counterPartyType,
            counterPartyUid,
            counterPartyName,
            counterPartySubEntityUid,
            counterPartySubEntityName,
            counterPartySubEntityIdentifier,
            counterPartySubEntitySubIdentifier,
            exchangeRate,
            totalFeeAmount_currency,
            totalFeeAmount_minorUnits,
            reference,
            country,
            spendingCategory,
            userNote,
            roundUp_goalCategoryUid,
            roundUp_amount_currency,
            roundUp_amount_minorUnits,
            hasAttachment,
            receiptPresent,
            feedItemFailureReason,
            sourceUid,
            webhookEventUid,
            eventTimestamp,
            accountHolderUid,
            last_modified
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        
        """
        
        # Prepare values tuple - just access the fields directly
        values = (
            payload.content.feedItemUid,
            payload.content.categoryUid,
            payload.content.accountUid,
            payload.content.amount.currency,
            payload.content.amount.minorUnits,
            payload.content.sourceAmount.currency,
            payload.content.sourceAmount.minorUnits,
            payload.content.direction,
            payload.content.updatedAt,
            payload.content.transactionTime,
            payload.content.settlementTime,
            payload.content.source,
            payload.content.status,
            payload.content.transactingApplicationUserUid,
            payload.content.counterPartyType,
            payload.content.counterPartyUid,
            payload.content.counterPartyName,
            payload.content.counterPartySubEntityUid,
            payload.content.counterPartySubEntityName,
            payload.content.counterPartySubEntityIdentifier,
            payload.content.counterPartySubEntitySubIdentifier,
            payload.content.exchangeRate,
            payload.content.totalFeeAmount.currency if payload.content.totalFeeAmount else None,
            payload.content.totalFeeAmount.minorUnits if payload.content.totalFeeAmount else None,
            payload.content.reference,
            payload.content.country,
            payload.content.spendingCategory,
            payload.content.userNote,
            payload.content.roundUp.goalCategoryUid if payload.content.roundUp else None,
            payload.content.roundUp.amount.currency if payload.content.roundUp else None,
            payload.content.roundUp.amount.minorUnits if payload.content.roundUp else None,
            payload.content.hasAttachment,
            payload.content.receiptPresent,
            payload.content.feedItemFailureReason,
            payload.content.sourceUid,
            payload.webhookEventUid,
            payload.eventTimestamp,
            payload.accountHolderUid
        )
        conn.execute(insert_query, values)
        conn.close()
        return True
        
    except Exception as e:
        log.error(f"Error inserting data into MotherDuck: {str(e)}")
        raise
    

def validate_webhook_auth(payload: WebhookPayload) -> bool:
    """Validate that the webhook is for the expected account UID"""
    if not payload.content.accountUid == ACCOUNT_UUID:
        log.error(f"Invalid account UID in webhook: {payload.content.accountUid}")
        raise Unauthorized('Invalid account UID')
    return True

@app.route('/starling/feed-item', methods=['POST'])
def receive_transaction_webhook():
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 'error',
                'message': 'No JSON payload provided'
            }), 400
        try:
            payload = WebhookPayload(**data)
        except ValidationError as e:
            return jsonify({
                'status': 'error',
                'errors': e.errors()
            }), 422
        validate_webhook_auth(payload)
        insert_webhook_data(payload)
        trigger_pipeline()
        log.info(f"Received webhook at {datetime.now()}")
        return jsonify({"status": "success"}), 200
    except Exception as e:
        log.error(f"Error processing webhook: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200
    
#if __name__ == '__main__':
#    #setup_logging()
#    app.run(debug=True, host='0.0.0.0', port=5000)
    