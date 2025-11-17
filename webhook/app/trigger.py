from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
from app.utils.logging_config import setup_logging

log = logging.getLogger(__name__)

async def trigger_pipeline_async(
    pipeline_name: str = "webhook-pipeline", 
    flow_name: str = "webhook-pipeline",
    timeout: int = 30
):
    from prefect import get_client
    async with get_client() as client:
        deployment_path = f"{flow_name}/{pipeline_name}"
        log.info(f"Calling Prefect client for deployment: {deployment_path}")
        
        try:
            async with asyncio.timeout(timeout):
                deployment = await client.read_deployment_by_name(deployment_path)
                
                if not deployment:
                    error_msg = f"Deployment '{deployment_path}' not found"
                    log.error(error_msg)
                    raise ValueError(error_msg)
                
                log.info(f"Found deployment: {deployment.name} (ID: {deployment.id})")
                
                flow_run = await client.create_flow_run_from_deployment(deployment.id)
                
                log.info(
                    f"Successfully created flow run {flow_run.id} for deployment '{deployment.name}'"
                )
                return flow_run
                
        except asyncio.TimeoutError:
            error_msg = f"Timeout after {timeout}s while triggering pipeline '{deployment_path}'"
            log.error(error_msg)
            raise TimeoutError(error_msg)
        except ValueError:
            raise
        except Exception as e:
            log.error(f"Failed to trigger pipeline '{deployment_path}': {str(e)}")
            raise


# Add sync wrapper
def trigger_pipeline(
    pipeline_name: str = "webhook-pipeline",
    flow_name: str = "webhook-pipeline",
    timeout: int = 30
):
    """Synchronous wrapper for triggering Prefect pipeline"""
    return asyncio.run(trigger_pipeline_async(pipeline_name, flow_name, timeout))

if __name__ == "__main__":
    setup_logging()
    trigger_pipeline()