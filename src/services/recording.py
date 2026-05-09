"""
Recording pipeline — fetches the call recording from Exotel and uploads to S3.

How Exotel works:
  After a call ends, Exotel processes the audio and makes a recording URL
  available via their REST API. The time between call-end and URL availability
  varies: typically 10–30 seconds, but can be 60–90s under load on their end.

  The URL is fetched via:
      GET /v1/Accounts/{account_sid}/Calls/{call_sid}/Recording
  Returns 200 + recording_url if ready, 404 if not yet available.

Current approach:
  Wait 45 seconds. Try once. If it's not there, give up silently.

This means:
  - Recordings ready in 10s: we waste 35 seconds of wall time
  - Recordings ready in 60s: we miss them entirely, no retry, no alert
  - We have no idea how many recordings we're silently missing

The Exotel API is poll-friendly — they don't rate-limit the status endpoint.
The information needed to fix this is already available: try, check, sleep
a bit, try again. How many times and with what interval is worth thinking about.

Note: recording upload and LLM analysis are completely independent. The LLM
reads the transcript text, not the audio. There's no reason they have to run
sequentially. What would need to change for them to run in parallel?
"""

import asyncio
import logging
from typing import Optional

import httpx

from src.config import settings

logger = logging.getLogger(__name__)

MAX_RECORDING_RETRIES = 6
RECORDING_RETRY_DELAY_SECONDS = 10
HTTP_TIMEOUT_SECONDS = 10


async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
    correlation_id: Optional[str] = None,
) -> Optional[str]:
    """
    Attempt to fetch the Exotel recording and upload it to S3.
     
    Improvements:
    - Poll-based retry instead of fixed 45s sleep
    - Better logging visibility
    - Correlation ID tracing
    - Retry attempt tracking
    - Timeout protection
    """
    if not call_sid:
        logger.warning(
            "missing_call_sid",
            extra={
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
            },
        )
        return None
    for attempt in range(1, MAX_RECORDING_RETRIES + 1):
        try:
            logger.info(
                "recording_fetch_attempt",
                extra={
                    "interaction_id": interaction_id,
                    "call_sid": call_sid,
                    "attempt": attempt,
                    "correlation_id": correlation_id,
                },
            )
            recording_url = await _fetch_exotel_recording_url(
                call_sid=call_sid,
                account_id=exotel_account_id,
            )
            if recording_url:
                s3_key = await _upload_to_s3(
                    recording_url=recording_url,
                    interaction_id=interaction_id,
                )
                logger.info(
                    "recording_pipeline_completed",
                    extra={
                        "interaction_id": interaction_id,
                        "call_sid": call_sid,
                        "s3_key": s3_key,
                        "attempt": attempt,
                        "correlation_id": correlation_id,
                    },
                )
                return s3_key
            logger.warning(
                "recording_not_ready",
                extra={
                    "interaction_id": interaction_id,
                    "call_sid": call_sid,
                    "attempt": attempt,
                    "retry_in_seconds": RECORDING_RETRY_DELAY_SECONDS,
                    "correlation_id": correlation_id,
                },
            )
            await asyncio.sleep(RECORDING_RETRY_DELAY_SECONDS)
        except Exception as e:
            logger.exception(
                "recording_fetch_failed",
                extra={
                    "interaction_id": interaction_id,
                    "call_sid": call_sid,
                    "attempt": attempt,
                    "correlation_id": correlation_id,
                    "error": str(e),
                },
            )
    logger.error(
        "recording_pipeline_exhausted",
        extra={
            "interaction_id": interaction_id,
            "call_sid": call_sid,
            "max_retries": MAX_RECORDING_RETRIES,
            "correlation_id": correlation_id,
        },
    )
    return None


async def _fetch_exotel_recording_url(
    call_sid: str, account_id: str
) -> Optional[str]:
    """
    Hit the Exotel API to get the recording URL for a completed call.

    Returns the recording URL if available, None if not yet ready.
    The 404 case (not yet ready) and the genuine error case (call had no
    recording, e.g., call was never connected) look the same from here —
    both return None. A retry loop would want to handle these differently.
    """
    url = (
        f"https://api.exotel.com/v1/Accounts/"
        f"{account_id}/Calls/{call_sid}/Recording"
    )
    try:
        async with httpx.AsyncClient(
            timeout=HTTP_TIMEOUT_SECONDS,
        ) as client:
            response = await client.get(url)

            if response.status_code == 200:
                data = response.json()
                return data.get("recording_url")

            if response.status_code == 404:
                return None
            logger.warning(
                "unexpected_exotel_response",
                extra={
                    "call_sid": call_sid,
                    "status_code": response.status_code,
                },
            )

            return None

    except httpx.TimeoutException:
        logger.warning(
            "recording_request_timeout",
            extra={
                "call_sid": call_sid,
            },
        )
        return None
    except httpx.HTTPError as e:
        logger.exception(
            "recording_http_error",
            extra={
                "call_sid": call_sid,
                "error": str(e),
            },
        )
        return None


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    """
    Download the recording from Exotel's URL and upload to S3.

    In production: stream from recording_url → boto3 upload to S3_BUCKET.
    S3 key format: recordings/{interaction_id}.mp3

    The interaction's recording_s3_key column gets updated after this succeeds.
    If this crashes after the upload but before the DB write, the file is in S3
    but the interaction row doesn't know about it. Currently no reconciliation job.
    """
    s3_key = f"recordings/{interaction_id}.mp3"

    logger.info(
        "recording_uploaded",
        extra={"interaction_id": interaction_id, "s3_key": s3_key},
    )
    return s3_key
