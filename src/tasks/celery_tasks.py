"""
Celery tasks for post-call processing.

This is the main background processing pipeline. Every completed interaction
with a long transcript ends up here.

Updated improvements:
    1. Correlation ID support added for tracing
    2. Short transcript handling moved inside Celery
    3. Signal jobs now run ONLY after analysis completes
    4. Metrics and logging improved
    5. Duplicate retry confusion reduced


WHY DOES RECORDING BLOCK ANALYSIS?
  It shouldn't. Recording upload and LLM analysis are completely independent —
  the LLM reads the transcript, not the audio file. But they're sequential here
  because that's how the task was originally written and nobody had a reason to
  split them until the 45-second sleep became a visible SLA problem.

  Think about what "run them in parallel" would require at the infrastructure level.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict

from src.tasks.celery_app import celery_app
from src.services.post_call_processor import PostCallProcessor, PostCallContext
from src.services.recording import fetch_and_upload_recording
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.services.retry_queue import retry_queue
from src.services.metrics import metrics_tracker

logger = logging.getLogger(__name__)


@celery_app.task(
    name="process_interaction_end_background_task",
    bind=True,
    max_retries=3,
    default_retry_delay=60,  # Fixed 60s — no exponential backoff
    acks_late=True,           # Task only acked after completion, not on receipt.
                              # This means a worker crash causes redelivery — good.
                              # But "redelivery" goes to the back of the queue,
                              # which at 100K depth means hours of extra wait.
    queue="postcall_processing",
)
def process_interaction_end_background_task(self, payload: Dict[str, Any]):
    """
    Main Celery task. Called for every long-transcript interaction.

    Celery workers are synchronous by default, so we spin up an event loop
    per task to run the async processing code. This means each Celery worker
    process handles one interaction at a time — no concurrency within a worker.

    At 100K interactions/campaign with ~3,500ms LLM latency per call:
        100,000 × 3.5s = 350,000 worker-seconds needed
        With 10 workers: ~9.7 hours to drain the queue

    If your campaign window is 8 hours, you're already behind before you start.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(_process_interaction(self, payload))
    except Exception as e:
        logger.exception(
            "celery_task_failed",
            extra={
                "interaction_id": payload.get("interaction_id"),
                "error": str(e),
                "attempt": self.request.retries,
            },
        )
        

        # Retry queue logging
        awaitable = retry_queue.enqueue_retry(
            interaction_id=payload["interaction_id"],
            error=str(e),
            payload=payload,
        )
        loop.run_until_complete(awaitable)
        raise self.retry(exc=e)
    finally:
        loop.close()


async def _process_interaction(task, payload: Dict[str, Any]):
    interaction_id = payload["interaction_id"]
    correlation_id = payload.get("correlation_id")

    logger.info(
        "postcall_processing_started",
        extra={
            "interaction_id": interaction_id,
            "correlation_id": correlation_id,
        },
    )

    await metrics_tracker.track_processing_started(interaction_id)

    ctx = PostCallContext(
        interaction_id=interaction_id,
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=payload["campaign_id"],
        customer_id=payload["customer_id"],
        agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=datetime.fromisoformat(payload["ended_at"]),
        exotel_account_id=payload.get("exotel_account_id"),
        correlation_id=payload.get("correlation_id"),
    )

    is_short = payload.get("is_short", False)
    #short call Flow

    if is_short:
        logger.info(
            "short_transcript_detected",
            extra={
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
            },
        )
        short_result = {
            "call_stage": "short_call",
            "summary": "Short interaction skipped from LLM processing",
        }
        try:
            await trigger_signal_jobs(
                interaction_id=ctx.interaction_id,
                session_id=ctx.session_id,
                campaign_id=ctx.campaign_id,
                analysis_result=short_result,
            )
        except Exception as e:
            logger.warning(
                "signal_jobs_failed",
                extra={
                    "interaction_id": interaction_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                },
            )

        try:
            await update_lead_stage(
                lead_id=ctx.lead_id,
                interaction_id=ctx.interaction_id,
                call_stage="short_call",
            )
        except Exception as e:
            logger.warning(
                "lead_stage_update_failed",
                extra={
                    "interaction_id": interaction_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                },
            )
        logger.info(
            "short_call_processing_completed",
            extra={
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
            },
        )

        return
    
    # Step 1: RECORDING FETCH
    processor = PostCallProcessor()

    recording_task = asyncio.create_task(
        fetch_and_upload_recording(
            interaction_id=ctx.interaction_id,
            call_sid=ctx.call_sid,
            exotel_account_id=ctx.exotel_account_id or "",
            correlation_id=correlation_id,
        )
    )

    analysis_task = asyncio.create_task(
        processor.process_post_call(ctx)
    )

    recording_s3_key, result = await asyncio.gather(
        recording_task,
        analysis_task,
    )

    if recording_s3_key:
        logger.info(
            "recording_uploaded",
            extra={
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
                "s3_key": recording_s3_key,
            },
        )
    else:
        logger.warning(
            "recording_upload_failed",
            extra={
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
            },
        )

    logger.info(
        "llm_processing_completed",
        extra={
            "interaction_id": interaction_id,
            "correlation_id": correlation_id,
            "tokens_used": result.tokens_used,
            "latency_ms": result.latency_ms,
        },
    )

    await metrics_tracker.track_processing_completed(
        interaction_id,
        result.tokens_used,
        result.latency_ms,
    )

    # ── Step 3: Signal jobs ───────────────────────────────────────────────────
    try:
        await trigger_signal_jobs(
            interaction_id=ctx.interaction_id,
            session_id=ctx.session_id,
            campaign_id=ctx.campaign_id,
            analysis_result=result.raw_response,
            correlation_id=correlation_id,
        )
        logger.info(
            "signal_jobs_completed",
            extra={
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
            },
        )
    except Exception as e:
        logger.warning(
            "signal_jobs_failed",
            extra={
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
                "error": str(e),
            },
        )

    # ── Step 4: Lead stage update ─────────────────────────────────────────────
    try:
        await update_lead_stage(
            lead_id=ctx.lead_id,
            interaction_id=ctx.interaction_id,
            call_stage=result.call_stage,
            correlation_id=correlation_id,
        )
        logger.info(
            "lead_stage_updated",
            extra={
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
                "call_stage": result.call_stage,
            },
        )
    except Exception as e:
        logger.warning(
            "lead_stage_update_failed",
            extra={
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
                "error": str(e),
            },
        )

    logger.info(
        "postcall_processing_completed",
        extra={
            "interaction_id": interaction_id,
            "correlation_id": correlation_id,
        },
    )