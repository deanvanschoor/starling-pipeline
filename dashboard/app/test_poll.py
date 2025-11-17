
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
) -> tuple[bool, str]:  
    """
    Check if any flows in pipeline_names have completed runs after last_refreshed.
    Returns (should_refresh, debug_message).
    """
    if pipeline_names is None:
        pipeline_names = ("main-pipeline", "webhook-pipeline")
    
    if last_refreshed_iso is None:
        last_refreshed_iso = st.session_state.last_refresh_time
    
    # Convert ISO string to datetime for comparison
    last_refreshed = datetime.fromisoformat(last_refreshed_iso)
    if last_refreshed.tzinfo is None:
        last_refreshed = last_refreshed.replace(tzinfo=timezone.utc)
    
    result, debug_msg = asyncio.run(_check_recent_runs(pipeline_names, last_refreshed, timeout))
    return result, debug_msg


async def _check_recent_runs(
    pipeline_names: tuple,
    last_refreshed: datetime,
    timeout: int
) -> tuple[bool, str]:
    """Async helper to check for recent flow runs."""
    from prefect import get_client
    from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterId
    
    debug_lines = []
    
    async with get_client() as client:
        debug_lines.append(f"🔌 Connected to Prefect")
        
        try:
            async with asyncio.timeout(timeout):
                # Get ALL deployments first
                all_deployments = await client.read_deployments()
                debug_lines.append(f"📦 Total deployments: {len(all_deployments)}")
                
                # Find matching deployments
                matching_deployments = [
                    d for d in all_deployments 
                    if d.name in pipeline_names
                ]
                
                debug_lines.append(f"✅ Matched: {[d.name for d in matching_deployments]}")
                
                if not matching_deployments:
                    msg = "\n".join(debug_lines + [f"⚠️ No matches for {pipeline_names}"])
                    return False, msg
                
                # Check for recent runs
                for deployment in matching_deployments:
                    debug_lines.append(f"\n🔎 Checking: {deployment.name}")
                    
                    deployment_filter = DeploymentFilter(
                        id=DeploymentFilterId(any_=[deployment.id])
                    )
                    
                    runs = await client.read_flow_runs(
                        deployment_filter=deployment_filter,
                        sort="EXPECTED_START_TIME_DESC",
                        limit=5
                    )
                    
                    debug_lines.append(f"   📋 Found {len(runs)} runs")
                    
                    for i, run in enumerate(runs):
                        # Make end_time timezone-aware
                        end_time = run.end_time
                        if end_time and end_time.tzinfo is None:
                            end_time = end_time.replace(tzinfo=timezone.utc)
                        
                        state_type = run.state.type if run.state else "NO_STATE"
                        is_completed = run.state.is_completed() if run.state else False
                        
                        debug_lines.append(f"   Run {i+1}: {run.name}")
                        debug_lines.append(f"      State: {state_type}, Completed: {is_completed}")
                        debug_lines.append(f"      Ended: {end_time}")
                        
                        if end_time:
                            is_newer = end_time > last_refreshed
                            debug_lines.append(f"      Is newer than {last_refreshed}? {is_newer}")
                            
                            if is_completed and is_newer:
                                debug_lines.append(f"   🎯 MATCH! Will refresh")
                                msg = "\n".join(debug_lines)
                                return True, msg
                
                debug_lines.append(f"\n❌ No new runs after {last_refreshed}")
                msg = "\n".join(debug_lines)
                return False, msg
                
        except asyncio.TimeoutError:
            error_msg = f"⏱️ Timeout after {timeout}s"
            debug_lines.append(error_msg)
            msg = "\n".join(debug_lines)
            return False, msg
        except Exception as e:
            error_msg = f"💥 Error: {str(e)}"
            debug_lines.append(error_msg)
            msg = "\n".join(debug_lines)
            return False, msg

if __name__ == "__main__":
    setup_logging()
    asyncio.run(poll_for_pipeline_run())