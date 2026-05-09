"""
Signal jobs — downstream actions triggered after post-call analysis.

Examples of what runs here in production:
  - Send a WhatsApp message to the lead ("Your appointment is confirmed for 3 PM tomorrow")
  - Book a callback slot in the scheduling system
  - Push the call outcome to the customer's CRM via webhook
  - Flag the interaction for human review if the lead was angry

These are the actions the business actually cares about. Getting the analysis
done is only valuable if these downstream triggers fire correctly and durably.

Updated improvements:
  1. correlation_id support added
  2. Empty analysis payload validation added
  3. Execution timing logs added
  4. Better structured logging added
  5. Failure visibility improved
"""


import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)

VALID_CALL_STAGES = {
    "rebook_confirmed",
    "not_interested",
    "callback_requested",
    "follow_up",
    "booked",
    "processing",
    "short_call",
    "unknown",
}

async def trigger_signal_jobs(
    interaction_id: str,
    session_id: str,
    campaign_id: str,
    analysis_result: Dict[str, Any],
    correlation_id: Optional[str] = None,
) -> None:
    """
    Dispatch downstream actions based on the call analysis.

    analysis_result contains call_stage and entities from the LLM.
   
    """
    started_at = datetime.now(timezone.utc)

    has_analysis = bool(analysis_result)
    if not has_analysis:
        logger.warning(
            "empty_analysis_payload_detected",
            extra={
                "interaction_id": interaction_id,
                "campaign_id": campaign_id,
                "correlation_id": correlation_id,
            },
        )
        return

    logger.info(
         "signal_jobs_started",
        extra={
            "interaction_id": interaction_id,
            "session_id": session_id,
            "campaign_id": campaign_id,
            "correlation_id": correlation_id,
            "call_stage": analysis_result.get("call_stage"),
        },
    )
    try:
        call_stage = analysis_result.get("call_stage", "unknown")
        entities = analysis_result.get("entities", {})
        summary = analysis_result.get("summary", "")
        logger.info(
            "signal_jobs_triggered",
            extra={
                "interaction_id": interaction_id,
                "campaign_id": campaign_id,
                "correlation_id": correlation_id,
                "call_stage": call_stage,
                "has_entities": bool(entities),
                "has_summary": bool(summary),
            },
        )
        # Mock downstream execution
        # Production:
        # - WhatsApp dispatch
        # - CRM webhook
        # - Callback scheduling
        # - Human review escalation

        elapsed_ms = (
            datetime.now(timezone.utc) - started_at
        ).total_seconds() * 1000

        logger.info(
            "signal_jobs_completed",
            extra={
                "interaction_id": interaction_id,
                "campaign_id": campaign_id,
                "correlation_id": correlation_id,
                "latency_ms": elapsed_ms,
            },
        )
    except Exception as e:
        logger.exception(
            "signal_jobs_processing_failed",
            extra={
                 "interaction_id": interaction_id,
                "campaign_id": campaign_id,
                "correlation_id": correlation_id,
                "error": str(e),
            },
        )
        raise

async def update_lead_stage(
    lead_id: str,
    interaction_id: str,
    call_stage: str,
    correlation_id: Optional[str] = None,
) -> None:
    """
    Update the lead's stage in the leads table.

    call_stage maps to a stage in the sales funnel:
      "rebook_confirmed" → "booked"
      "not_interested" → "closed_lost"
      "callback_requested" → "follow_up"
    """
    started_at = datetime.now(timezone.utc)

    if not call_stage:
        logger.warning(
            "missing_call_stage",
            extra={
                "lead_id": lead_id,
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
            },
        )
        return

    if call_stage not in VALID_CALL_STAGES:
        logger.warning(
            "invalid_call_stage_detected",
            extra={
                "lead_id": lead_id,
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
                "call_stage": call_stage,
            },
        )
        return

    logger.info(
        "lead_stage_update_started",
        extra={
            "lead_id": lead_id,
            "interaction_id": interaction_id,
            "correlation_id": correlation_id,
            "call_stage": call_stage,
        },
    )

    try:
        # Mock database update
        # Production:
        # UPDATE leads SET stage = $2 WHERE id = $1

        elapsed_ms = (
            datetime.now(timezone.utc) - started_at
        ).total_seconds() * 1000

        logger.info(
            "lead_stage_updated",
            extra={
                "lead_id": lead_id,
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
                "new_stage": call_stage,
                "latency_ms": elapsed_ms,
            },
        )

    except Exception as e:
        logger.exception(
            "lead_stage_update_failed",
            extra={
                "lead_id": lead_id,
                "interaction_id": interaction_id,
                "correlation_id": correlation_id,
                "call_stage": call_stage,
                "error": str(e),
            },
        )
        raise








