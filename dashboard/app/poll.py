from dotenv import load_dotenv
load_dotenv()
import streamlit as st
import asyncio
from datetime import datetime, timezone
import logging

from app.utils.logging_config import setup_logging

log = logging.getLogger(__name__)

@st.cache_data(ttl=2)
def poll_for_pipeline_run(
    pipeline_names: tuple = None,  
    last_refreshed_iso: str = None,
    timeout: int = 30
) -> bool:
    """
    Check if any flows in pipeline_names have completed runs after last_refreshed.
    Returns True if a refresh is needed, False otherwise.
    """
    if pipeline_names is None:
        pipeline_names = ("main-pipeline", "webhook-pipeline")
    
    if last_refreshed_iso is None:
        last_refreshed_iso = st.session_state.last_refresh_time
    
    # Convert ISO string to datetime for comparison
    last_refreshed = datetime.fromisoformat(last_refreshed_iso)
    
    # Run the async function in a new event loop (for Streamlit compatibility)
    return asyncio.run(_check_recent_runs(pipeline_names, last_refreshed, timeout))


async def _check_recent_runs(
    pipeline_names: tuple,
    last_refreshed: datetime,
    timeout: int
) -> bool:
    from prefect import get_client
    from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterId
    """Async helper to check for recent flow runs."""
    async with get_client() as client:
        log.info(f"Polling Prefect for deployments: {pipeline_names}")
        
        try:
            async with asyncio.timeout(timeout):
                deployments = await client.read_deployments()
                matching_deployments = [
                    d for d in deployments 
                    if d.name in pipeline_names
                ]
                
                if not matching_deployments:
                    log.warning(f"No deployments found matching: {pipeline_names}")
                    return False
                
                # Check for recent runs across all matching deployments
                for deployment in matching_deployments:
                    # Create proper filter object using Prefect's filter classes
                    deployment_filter = DeploymentFilter(
                        id=DeploymentFilterId(any_=[deployment.id])
                    )
                    
                    runs = await client.read_flow_runs(
                        deployment_filter=deployment_filter,
                        sort="EXPECTED_START_TIME_DESC",
                        limit=5
                    )
                    
                    for run in runs:
                        if (run.state and 
                            run.state.is_completed() and 
                            run.end_time and 
                            run.end_time > last_refreshed):
                            
                            log.info(
                                f"Found recent run for '{deployment.name}': "
                                f"{run.name} ended at {run.end_time}"
                            )
                            return True
                
                log.info(f"No recent completed runs found for {pipeline_names}")
                return False
                
        except asyncio.TimeoutError:
            error_msg = f"Timeout after {timeout}s polling {pipeline_names}"
            log.error(error_msg)
            raise TimeoutError(error_msg)
        except Exception as e:
            log.error(f"Failed to poll Prefect: {str(e)}")
            raise
  
if __name__ == "__main__":
    setup_logging
    asyncio.run(poll_for_pipeline_run(("main-pipeline", "webhook-pipeline"), datetime.now(timezone.utc), 30))      
        