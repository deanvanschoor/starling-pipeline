from prefect import flow, serve
import logging

from app.flows.balance import balance_dag
from app.flows.spaces import spaces_dag
from app.flows.transactions import transactions_dag, insert_webhook_to_staging


logger = logging.getLogger(__name__)

@flow(name="webhook-pipeline", log_prints=True, description="Main pipeline to orchestrate all data flows",timeout_seconds=1800)
def webhook_pipeline():
    logger.info("Starting webhook pipeline")
    balance_dag()
    spaces_dag()
    insert_webhook_to_staging()
    
@flow(name="main-pipeline", log_prints=True, description="Main pipeline to orchestrate all data flows",timeout_seconds=7200)
def main_pipeline():
    logger.info("Starting main pipeline")
    transactions_dag()
    balance_dag()
    spaces_dag()


if __name__ == "__main__":
#    from app.utils.logging_config import setup_logging
#    setup_logging()
#    main_pipeline()

    #main_pipeline.serve(
    #name="main-pipeline",
    #tags=["banking-app", "dev"],
    #description="Main pipeline to orchestrate transactions, balance, and spaces dags",
    #version="1.0.0",
    #)
    
    #main_pipeline.deploy(
    #    name="main-pipeline",
    #    work_pool_name="default-agent-pool",
    #    tags=["banking-app", "dev"],
    #    description="Main pipeline to orchestrate transactions, balance, and spaces dags",
    #    version="1.0.0",
    #)
    
    main = main_pipeline.to_deployment(
            name="main-pipeline",
            tags=["banking-app", "dev"],
            description="Main pipeline to orchestrate transactions, balance, and spaces dags",
            version="1.0.0",
            )
    webhook = webhook_pipeline.to_deployment(
            name="webhook-pipeline",
            tags=["banking-app", "dev"],
            description="webhook pipeline to orchestrate webhook transactions, balance, and spaces dags",
            version="1.0.0",
            )
    serve(main, webhook)