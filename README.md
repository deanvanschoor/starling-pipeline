# Real-time(ish) Personal Finance Analytics Platform

An application that ingests banking transactions via API and webhooks, transforms and visualises insights in an dashboard.

## Pipeline Architecture
```
Starling API + Webhooks
           ↓
    Prefect Pipelines
           ↓
MotherDuck (Landing → Staging → Semantic)
           ↓
   Streamlit Dashboard
```

## Structure

```
.
├── orchestrator/          # Prefect workflows and data pipelines
│   ├── app/
│   │   ├── flows/        # Pipeline definitions (transactions, balance, spaces)
│   │   ├── tasks/        # Reusable task components (API calls, SQL execution)
│   │   └── utils/        # Logging and configuration utilities
│   └── dockerfile
│
├── webhook/              # Flask webhook endpoint
│   ├── app/
│   │   ├── models.py     # Pydantic validation models
│   │   ├── webhook.py    # Flask application
│   │   └── trigger.py    # Prefect pipeline trigger logic
│   └── dockerfile
│
├── dashboard/            # Streamlit analytics dashboard
│   ├── app/
│   │   ├── dashboard.py  # Main dashboard application
│   │   ├── poll.py       # Pipeline completion polling
│   │   └── constants.py  # Database connection configuration
│   └── dockerfile
│
├── database/             # SQL schema definitions
│   ├── schema/          # Schema creation scripts
│   ├── tables/          # Table definitions (landing, staging)
│   ├── views/           # Semantic layer views
│   └── index/           # Performance indexes
│
├── docker-compose.yml    # Local development setup
├── docker-compose.prod.yml  # Production deployment
└── Caddyfile            # Reverse proxy configuration
```

## Quick Start

### Prerequisites

Before running the application, you'll need to set up the following:

#### 1. MotherDuck Setup
1. Create a free account at [motherduck.com](https://motherduck.com)
2. Generate a service token from your MotherDuck dashboard (Settings → Service Tokens)
3. Create the required database objects:
```sql
   -- Connect to MotherDuck and run:
   CREATE DATABASE IF NOT EXISTS database_name;
   
   -- Create your tables, views, and other objects
   -- (Refer to database folder in the project for full schema)
```

#### 2. Starling Bank API Setup
1. Log into your Starling Bank account
2. Navigate to the Developer section and create a **Personal Access Token**
3. Note down your token (keep this secure)
4. Find your Account UUID:
   - Use the Starling API docs to call `GET /api/v2/accounts`
   - Copy the `accountUid` from the response
5. Set up the webhook:
   - Using your Personal Access Token, configure a webhook endpoint
   - Point it to your webhook URL: `https://webhook.yourdomain.com/starling/feed-items` (production) or your ngrok/tunnel URL (development)
   - Refer to [Starling's Webhook Documentation](https://developer.starlingbank.com/docs) for detailed instructions

### Environment Configuration

Create a `.env` file in the project root with the credentials from above:
```bash
# MotherDuck - Service token from step 1
MD_TOKEN=

# Starling Bank API - Personal Access Token from step 2
STARLING_TOKEN=

# Starling Account UUID - from step 2.4
ACCOUNT_UUID=

# Production Settings (optional for local development)
EMAIL=
DOMAIN=
```

### Local Development
```bash
# Start all services
docker-compose up -d

# Access services:
# - Prefect UI: http://localhost:4200
# - Dashboard: http://localhost:8501
# - Webhook: http://localhost:5000

# For webhook testing, use ngrok or similar to expose port 5000
# Then update your Starling webhook URL to point to the ngrok URL
```

### Production Deployment
```bash
# Use production compose file
docker-compose -f docker-compose.prod.yml up -d

# Services will be available at:
# - Prefect: https://prefect.yourdomain.com
# - Dashboard: https://yourdomain.com/dashboard
# - Webhook: https://webhook.yourdomain.com/starling/feed-items

# Ensure your Starling webhook is configured to use the production webhook URL
```

