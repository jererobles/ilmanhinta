"""API endpoints for triggering Dagster jobs from the web interface."""

import asyncio
import os
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

from dagster_graphql import DagsterGraphQLClient, DagsterGraphQLClientError
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ilmanhinta.db.postgres_client import PostgresClient
from ilmanhinta.logging import get_logger

router = APIRouter(tags=["dagster"])
logger = get_logger(__name__)

# Dagster configuration - use environment variables or defaults
DAGSTER_HOST = os.getenv("DAGSTER_HOST", "localhost")
DAGSTER_PORT = os.getenv("DAGSTER_PORT", "3000")
DAGSTER_USE_HTTPS = os.getenv("DAGSTER_USE_HTTPS", "false").lower() in {"1", "true", "yes"}
DAGSTER_GRAPHQL_TIMEOUT = int(os.getenv("DAGSTER_GRAPHQL_TIMEOUT", "30"))


class JobTriggerResponse(BaseModel):
    """Response from triggering a Dagster job."""

    status: str
    run_id: str | None
    message: str
    timestamp: datetime


class JobStatusResponse(BaseModel):
    """Status of a Dagster run."""

    run_id: str
    status: str
    started_at: datetime | None
    ended_at: datetime | None


# Job name mappings (from UI name to actual Dagster job name)
JOB_MAPPINGS = {
    # Consumption pipeline (new canonical names + legacy aliases)
    "consumption_ingest_job": "consumption_ingest_job",
    "ingest_data": "consumption_ingest_job",
    "consumption_train_job": "consumption_train_job",
    "train_models": "consumption_train_job",
    "consumption_forecast_job": "consumption_forecast_job",
    "forecast_next_24h": "consumption_forecast_job",
    "consumption_bootstrap_job": "consumption_bootstrap_job",
    "bootstrap_system": "consumption_bootstrap_job",
    "consumption_short_interval_job": "consumption_short_interval_job",
    "scrape_short_interval": "consumption_short_interval_job",
    # Price pipeline
    "price_hourly_prediction_job": "price_hourly_prediction_job",
    "hourly_prediction_job": "price_hourly_prediction_job",
    "price_daily_analysis_job": "price_daily_analysis_job",
    "daily_analysis_job": "price_daily_analysis_job",
    "price_weekly_retraining_job": "price_weekly_retraining_job",
    "weekly_retraining_job": "price_weekly_retraining_job",
    "price_bootstrap_job": "price_bootstrap_job",
    "bootstrap_price_pipeline": "price_bootstrap_job",
}

_GRAPHQL_ERROR_STATUS_MAP = {
    "RunConfigValidationInvalid": 400,
    "PipelineConfigValidationInvalid": 400,
    "InvalidStepError": 400,
    "InvalidOutputError": 400,
    "RunNotFoundError": 404,
    "JobNotFoundError": 404,
    "PipelineNotFoundError": 404,
    "DagsterRunConflict": 409,
    "RunConflict": 409,
}


def _parse_port(port_value: str | None) -> int | None:
    if not port_value:
        return None
    try:
        return int(port_value)
    except ValueError:
        logger.warning(
            "Invalid DAGSTER_PORT '%s'. Falling back to default client behavior.", port_value
        )
        return None


@lru_cache(maxsize=1)
def get_dagster_client() -> DagsterGraphQLClient:
    """Create a cached Dagster GraphQL client to avoid re-connecting per request."""
    return DagsterGraphQLClient(
        hostname=DAGSTER_HOST,
        port_number=_parse_port(DAGSTER_PORT),
        use_https=DAGSTER_USE_HTTPS,
        timeout=DAGSTER_GRAPHQL_TIMEOUT,
    )


def _format_error_component(component: Any) -> str:
    if isinstance(component, list):
        return ", ".join(_format_error_component(item) for item in component if item)
    if isinstance(component, dict):
        message = component.get("message")
        path = component.get("path")
        if isinstance(path, list) and path:
            return (
                f"{message} ({'.'.join(str(p) for p in path)})"
                if message
                else ".".join(str(p) for p in path)
            )
        return str(message) if message else str(component)
    return str(component)


def _handle_dagster_error(error: DagsterGraphQLClientError) -> None:
    error_type = error.args[0] if error.args else "DagsterGraphQLClientError"
    detail_parts = [_format_error_component(component) for component in error.args[1:]]
    if error.body:
        detail_parts.append(_format_error_component(error.body))
    detail = "; ".join(part for part in detail_parts if part)
    if "Error when connecting to url" in error_type:
        raise HTTPException(status_code=503, detail=error_type) from error

    status_code = _GRAPHQL_ERROR_STATUS_MAP.get(error_type, 500)
    detail_text = f"{error_type}: {detail}" if detail else error_type
    raise HTTPException(status_code=status_code, detail=detail_text) from error


async def _execute_dagster_query(
    query: str, variables: dict[str, Any] | None = None
) -> dict[str, Any]:
    client = get_dagster_client()
    try:
        # DagsterGraphQLClient only exposes a private _execute for custom queries.
        return await asyncio.to_thread(client._execute, query, variables)
    except DagsterGraphQLClientError as dagster_error:
        _handle_dagster_error(dagster_error)
        raise HTTPException(status_code=500, detail="Unknown Dagster error") from dagster_error


# Special endpoint for refreshing materialized views (not a Dagster job)
@router.post("/trigger-job/refresh_views", response_model=JobTriggerResponse)
async def refresh_materialized_views() -> JobTriggerResponse:
    """
    Special endpoint to refresh PostgreSQL materialized views.

    This is not a Dagster job but a direct database operation.
    """
    db = PostgresClient()

    try:
        db.refresh_price_analytics_views()
        logger.info("Materialized views refreshed successfully")

        return JobTriggerResponse(
            run_id=None,
            status="success",
            message="Comparison views refreshed",
            timestamp=datetime.now(UTC),
        )

    except Exception as e:
        logger.error(f"Error refreshing views: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to refresh views: {str(e)}") from e

    finally:
        db.close()


@router.post("/trigger-job/{job_name}", response_model=JobTriggerResponse)
async def trigger_dagster_job(job_name: str) -> JobTriggerResponse:
    """
    Trigger a Dagster job manually.

    Available jobs:
    - **Consumption Pipeline:**
      - `ingest_data`: Fetch latest electricity and weather data
      - `train_models`: Retrain the LightGBM consumption model
      - `forecast_next_24h`: Generate 24-hour consumption forecast
      - `bootstrap_system`: Full pipeline initialization
      - `scrape_short_interval`: Short-interval data scrape

    - **Price Pipeline:**
      - `hourly_prediction_job`: Update price predictions
      - `daily_analysis_job`: Generate performance reports
      - `weekly_retraining_job`: Retrain XGBoost price model
    """
    # Map UI name to actual job name
    actual_job_name = JOB_MAPPINGS.get(job_name)
    if not actual_job_name:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown job: {job_name}. Available jobs: {list(JOB_MAPPINGS.keys())}",
        )

    logger.info(f"Triggering Dagster job: {actual_job_name}")

    try:
        client = get_dagster_client()
        run_id = await asyncio.to_thread(client.submit_job_execution, actual_job_name)
        logger.info(f"Successfully triggered job {actual_job_name} with run ID: {run_id}")

        return JobTriggerResponse(
            status="success",
            run_id=run_id,
            message=f"Job {job_name} triggered successfully",
            timestamp=datetime.now(UTC),
        )

    except HTTPException:
        raise
    except DagsterGraphQLClientError as dagster_error:
        _handle_dagster_error(dagster_error)
        raise HTTPException(status_code=500, detail="Unknown Dagster error") from dagster_error
    except Exception as e:
        logger.error(f"Unexpected error triggering job {actual_job_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger job: {str(e)}") from e


@router.get("/job-status/{run_id}", response_model=JobStatusResponse)
async def get_job_status(run_id: str) -> JobStatusResponse:
    """
    Get the status of a Dagster run by its ID.

    Returns the current status and timing information for the specified run.
    """
    query = """
    query GetRun($runId: ID!) {
        runOrError(runId: $runId) {
            __typename
            ... on Run {
                runId
                status
                startTime
                endTime
            }
            ... on RunNotFoundError {
                message
            }
        }
    }
    """

    try:
        result = await _execute_dagster_query(query, {"runId": run_id})
        run_or_error = result.get("runOrError", {})
        typename = run_or_error.get("__typename")

        if typename == "Run":
            start_time = run_or_error.get("startTime")
            end_time = run_or_error.get("endTime")

            return JobStatusResponse(
                run_id=run_or_error.get("runId"),
                status=run_or_error.get("status"),
                started_at=(datetime.fromtimestamp(start_time, tz=UTC) if start_time else None),
                ended_at=datetime.fromtimestamp(end_time, tz=UTC) if end_time else None,
            )

        elif typename == "RunNotFoundError":
            raise HTTPException(
                status_code=404,
                detail=f"Run {run_id} not found: {run_or_error.get('message')}",
            )

        else:
            raise HTTPException(
                status_code=500, detail=f"Unexpected response type: {typename or 'unknown'}"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job status for run {run_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get job status: {str(e)}") from e


@router.get("/jobs/list")
async def list_available_jobs() -> dict[str, Any]:
    """
    List all available Dagster jobs that can be triggered.

    Returns job names, descriptions, and their last run status.
    """
    query = """
    query ListJobs {
        repositoriesOrError {
            __typename
            ... on RepositoryConnection {
                nodes {
                    name
                    pipelines {
                        name
                        description
                        runs(limit: 1) {
                            runId
                            status
                            startTime
                        }
                    }
                }
            }
        }
    }
    """

    try:
        result = await _execute_dagster_query(query)
        repos_or_error = result.get("repositoriesOrError", {})
        if repos_or_error.get("__typename") != "RepositoryConnection":
            raise HTTPException(status_code=500, detail="Failed to fetch repository information")

        jobs = []
        for repo in repos_or_error.get("nodes", []):
            for pipeline in repo.get("pipelines", []):
                job_name = pipeline.get("name")
                if job_name in JOB_MAPPINGS.values():
                    last_run = None
                    runs = pipeline.get("runs", [])
                    if runs:
                        run = runs[0]
                        last_run = {
                            "run_id": run.get("runId"),
                            "status": run.get("status"),
                            "started_at": (
                                datetime.fromtimestamp(run.get("startTime"), tz=UTC).isoformat()
                                if run.get("startTime")
                                else None
                            ),
                        }

                    # Find the UI name for this job
                    ui_name = next((k for k, v in JOB_MAPPINGS.items() if v == job_name), job_name)

                    jobs.append(
                        {
                            "name": ui_name,
                            "dagster_name": job_name,
                            "description": pipeline.get("description") or "No description",
                            "last_run": last_run,
                        }
                    )

        return {
            "jobs": jobs,
            "total": len(jobs),
            "timestamp": datetime.now(UTC).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}") from e
