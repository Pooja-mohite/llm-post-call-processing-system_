"""
PostCallRetryQueue — A Redis list used to retry failed post-call tasks.

This was added after we started seeing silent task failures in production.
The intention: if a Celery task fails, park it here and try again later.

The problem: this queue has the same durability as the thing it's retrying.
Both live in Redis. A Redis restart loses both the Celery broker queue AND
this retry queue simultaneously. We added a backup mechanism that has the
same failure mode as the thing it's backing up.

Other issues worth noting:
  - dequeue_ready() is not atomic. Two workers calling it at the same time
    can pop and process the same entry. Interactions can be analysed twice.
  - The state key (postcall:retry_state:{interaction_id}) has no TTL.
    Interactions that exhaust retries leave their state key in Redis forever.
  - Fixed 60-second retry delay. Whether we're retrying a 429 (should wait
    less) or a transient DB failure (should wait more) doesn't matter here.
  - When max retries is exceeded, the task is dropped with an error log.
    There is no dead-letter queue, no alerting, no way to replay it later
    without finding the original payload in the error log.
"""

import json
import logging
import time
from dataclasses import dataclass
from typing import List

from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)

RETRY_QUEUE_KEY = "postcall:retry_queue"
RETRY_STATE_PREFIX = "postcall:retry_state:"

# Retry state cleanup after 24 hours
RETRY_STATE_TTL_SECONDS = 86400

@dataclass
class RetryEntry:
    interaction_id: str
    attempt: int
    last_error: str
    next_retry_at: float
    payload: dict


class PostCallRetryQueue:

    def __init__(self, max_retries: int = 3, retry_delay_seconds: int = 60):
        self.max_retries = max_retries
        self.retry_delay = retry_delay_seconds  

    async def enqueue_retry(
        self, interaction_id: str, error: str, payload: dict,
   ) -> bool:
        """
        Push a failed interaction onto the retry queue.
        """
        state_key = f"{RETRY_STATE_PREFIX}{interaction_id}"
        current_attempt = int(await redis_client.get(state_key) or 0)

        if current_attempt >= self.max_retries:
            logger.error(
                "retry_exhausted",
                extra={
                    "interaction_id": interaction_id,
                    "attempts": current_attempt,
                    "last_error": error,
                    # The payload containing the full transcript and context
                    # is dropped here. There's no dead-letter store.
                    # If you need to replay this interaction, you have to find
                    # the original payload in logs and manually re-enqueue it.
                },
            )
            return False

        next_attempt = current_attempt + 1
        # Exponential backoff
        retry_delay = (
            self.retry_delay * (2 ** (next_attempt - 1))
        )
        entry = {
            "interaction_id": interaction_id,
            "attempt": next_attempt,
            "last_error": error,
            "next_retry_at": time.time() + retry_delay,
            "payload": payload,
        }

        # Store retry state with TTL
        await redis_client.set(
            state_key,
            next_attempt,
            ex=RETRY_STATE_TTL_SECONDS,
        )
        # No TTL set on state_key — this key lives in Redis indefinitely
        # for interactions that exhaust their retries.

        await redis_client.rpush(RETRY_QUEUE_KEY, json.dumps(entry),)

        logger.info(
            "retry_enqueued",
            extra={
                "interaction_id": interaction_id,
                "attempt": next_attempt,
                "retry_delay_seconds": retry_delay,
                "next_retry_at": entry["next_retry_at"],
            },
        )
        return True

    async def dequeue_ready(self) -> List[RetryEntry]:
        """
        Return all retry entries whose retry time has passed.
        """

        now = time.time()

        ready = []

        queue_length = await redis_client.llen(RETRY_QUEUE_KEY)
        for _ in range(queue_length):
            raw = await redis_client.lpop(RETRY_QUEUE_KEY)
            if not raw:
                break

            entry = json.loads(raw)
            # Malformed payload protection
            if not entry.get("interaction_id"):
                logger.warning(
                    "invalid_retry_entry_skipped",
                )
                continue
            if entry["next_retry_at"] <= now:
                ready.append(RetryEntry(**entry))
            else:
               # Push back if not ready
                await redis_client.rpush(RETRY_QUEUE_KEY, raw,)

        return ready

    async def get_queue_depth(self) -> int:
        """
        Returns the number of entries waiting to be retried.
        This is one of the only queue visibility metrics we have — and it's
        not exposed to any dashboard or alert. You'd have to query Redis manually.
        """
        return await redis_client.llen(RETRY_QUEUE_KEY)


retry_queue = PostCallRetryQueue()
