# Post-Call Processing Pipeline — Design Document

**Author:** Pooja Mohite
**Date:** 6 May 2026

---

## 1. Assumptions

_State every assumption you made about the business, system, or environment. Be specific. These will be discussed in the follow-up._

1. Around 100K calls are processed daily and many calls may end at the same time.

2. The system supports multiple customers, so one customer’s traffic should not affect others.

3. The LLM provider has request-per-minute (RPM) and token-per-minute (TPM) limits.

4. Some calls are more important and need faster processing.

5. Call recordings and transcripts may not be immediately available after the call ends.

6. Losing any completed call event is not acceptable.

7. PostgreSQL is available for durable storage.

8. Redis can be used for fast queue operations but not as the main permanent storage.

9. Delayed processing is acceptable, but complete system failure is not.

10. Engineers should be able to track and debug failed jobs later.

---

## 2. Problem Diagnosis

The current system starts LLM processing immediately whenever a call ends.

Current flow:

Call End Webhook → asyncio.create_task() → LLM API
This works fine for small traffic, but during heavy traffic many calls finish together and thousands of LLM requests are triggered at once.

Main problems in the current system:

Too many requests hit the LLM provider together.

Provider rate limits get exceeded.

Failed requests retry again and increase load.

Redis queue becomes overloaded.

Important tasks and normal tasks are treated the same.

No proper tracking exists for failed jobs.

If workers crash, some tasks may be lost.

The main issue is that there is no central scheduling or rate-control system.

---

## 3. Architecture Overview

_End-to-end flow from call-end webhook to completed analysis. Include a diagram._

Proposed Flow :

Call End Webhook
        ↓
Save Event in Postgres
        ↓
Classify Task (Priority + Token Estimate)
        ↓
Central Scheduler
        ↓
Priority Queues
        ↓
Worker Processing
        ↓
Store Results
        ↓
Retry / Dead Letter Queue

Architecture Diagram:
                ┌───────────────────┐
                │ Call End Webhook  │
                └─────────┬─────────┘
                          │
                          ▼
                ┌───────────────────┐
                │ Durable Storage   │
                │ (PostgreSQL)      │
                └─────────┬─────────┘
                          │
                          ▼
                ┌───────────────────┐
                │ Classification    │
                │ Priority + Tokens │
                └─────────┬─────────┘
                          │
                          ▼
                ┌───────────────────┐
                │ Central Scheduler │
                └─────────┬─────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
      High Queue      Normal Queue    Deferred Queue

                          ▼
                ┌───────────────────┐
                │ Worker Pool       │
                │ LLM Processing    │
                └─────────┬─────────┘
                          │
                          ▼
                ┌───────────────────┐
                │ Result Storage    │
                └───────────────────┘


### Key design decisions

1. Separate ingestion from processing
The webhook only stores the event first and returns success quickly.
Actual processing happens later through queues and workers.
This prevents sudden traffic bursts from directly hitting the LLM provider.
2. Use a central scheduler
A scheduler controls:
how many requests can run,
which customer gets capacity,
and which tasks should wait.
Without scheduling, burst traffic can overload the system.
3. Use priority-based processing
Important calls should not wait behind analytics or low-priority tasks.
4. Use durable storage
All tasks and states are stored in PostgreSQL so jobs are not lost if workers restart.

---

## 4. Rate Limit Management

_This is the primary problem. How does your system respect LLM rate limits across 100K calls?_
This is the most important part of the solution.

### How you track rate limit usage
How rate limits are tracked
The scheduler keeps track of:
1. requests per minute,
2. tokens per minute,
3. retries,
4. and queue size.
Each task also stores estimated token usage.
Example:
{
  "customer": "A",
  "estimated_tokens": 5000
}

### How you decide what to process now vs. defer
How processing decisions are made
Before sending work to the LLM:
1. scheduler checks available TPM and RPM,
2. checks task priority,
3. checks customer limits.
If enough capacity is available, the task is processed.
Otherwise, it stays in queue.

### What happens when the limit is hit (recovery, not crash)
What happens when limits are reached
The system does not crash.
Instead:
1. low-priority tasks are delayed,
2. retries use backoff,
3. workers slow down automatically,
4. queues temporarily hold pending work.
This keeps the platform stable.
---

## 5. Per-Customer Token Budgeting

_If total capacity is N tokens/min and K customers are active simultaneously:_

- How do you allocate capacity across customers?
Different customers should get fair access to the system.
Capacity allocation
Each customer gets:
1. reserved token budget,
2. optional burst capacity.

Example:
Customer	Reserved TPM
Customer A	300K
Customer B	150K

- What guarantees does a customer with a pre-allocated budget receive?
Customers with reserved capacity will continue getting processing even during heavy traffic.

- What happens when a customer exceeds their budget?
Extra requests are delayed and moved to lower-priority queues.
This prevents one customer from using all system capacity.

- What happens to unallocated headroom?
If some customers are inactive, their unused capacity can be temporarily shared with others.

---

## 6. Differentiated Processing

_Some call outcomes are time-sensitive. Some can wait. How do you determine which is which?_
Not all calls need immediate processing.
Priority Levels
Priority	                  Example
  P0	              Fraud or compliance alerts
  P1	              Customer escalations
  P2	              Standard summaries
  P3	              Analytics reports

_What mechanism do you use — is it a classification step, a flag set by the business, something else? Justify your choice._
Priority can come from:
1. business rules,
2. customer settings,
3. webhook metadata.
Example:
{
  "call_type": "compliance",
  "priority": "P0"
}
Business-defined priorities are easier to manage and debug.

---

## 7. Recording Pipeline

_Replacement for `asyncio.sleep(45s)`. How does it work? What does a failure look like to the on-call engineer?_

### Implemented Improvement

The fixed `asyncio.sleep(45)` approach was replaced with a poll-based retry mechanism.

New flow:

Call Ends
   ↓
Fetch Recording URL
   ↓
If 404 → Wait + Retry
   ↓
If Ready → Upload to S3

Implementation details:
- Maximum retries: 6
- Retry interval: 10 seconds
- HTTP timeout protection added
- Structured logging added for every retry attempt
- Correlation ID added for tracing

Benefits:
1. Recordings ready early are processed faster.
2. Late recordings are no longer silently missed.
3. Engineers can now track retry attempts through logs.
4. Failures are visible instead of silently ignored.

### Parallel Processing Improvement

Recording upload and LLM analysis were previously executed sequentially.

This caused LLM processing to wait unnecessarily for recording availability.

The implementation was updated to run:
- recording fetch/upload
- and LLM analysis

in parallel using `asyncio.gather()`.

Benefits:
1. Lower end-to-end latency
2. Better worker utilization
3. Faster post-call processing during traffic spikes

Failure handling
If recording is unavailable for too long:
1. task moves to failed state,
2. alert is generated,
3. retry details are stored.
On-call engineers can easily investigate the issue.

---

## 8. Reliability & Durability

_How do you ensure no analysis result is permanently lost?_

To prevent data loss:
1. Webhook events are stored before acknowledgement.
2. All workflow states are stored in database.
3. Failed jobs are retried safely.
4. Dead Letter Queue (DLQ) stores permanently failed tasks.
5. Idempotency keys prevent duplicate processing.
6. Retry state keys now use TTL cleanup to avoid indefinite Redis growth.
7. Retry delays now use exponential backoff instead of fixed retry intervals.
This ensures tasks are not lost even if workers crash.

---

## 9. Auditability & Observability

_How would you debug a specific failed interaction 3 days after the fact?_

The system should be easy to debug.
### What you log (and what fields every log event includes)
{
  "call_id": "",
  "customer_id": "",
  "status": "",
  "priority": "",
  "retry_count": "",
  "correlation_id": "",
  "timestamp": ""
}
Important Metrics
1. Queue size
2. Retry count
3. Failed jobs
4. Processing latency
5. Provider 429 errors
6. Token usage

### Alert conditions
| Alert               | Condition                     |
| ------------------- | ----------------------------- |
| Queue overload      | Queue grows too large         |
| Retry storm         | Retry rate suddenly increases |
| Provider throttling | Too many 429 errors           |
| DLQ growth          | Failed jobs increase          |
| Worker failure      | No processing activity        |


---

## 10. Data Model

_Schema changes required. Show the SQL._

```sql
-- Your schema additions/changes here
```
CREATE TABLE processing_jobs (
    id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    call_id TEXT UNIQUE NOT NULL,
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    estimated_tokens INT,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE processing_events (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID,
    event_type TEXT,
    payload JSONB,
    created_at TIMESTAMP
);

CREATE TABLE customer_budgets (
    customer_id UUID PRIMARY KEY,
    reserved_tpm INT,
    burst_limit INT
);

CREATE TABLE dead_letter_queue (
    id UUID PRIMARY KEY,
    job_id UUID,
    reason TEXT,
    payload JSONB,
    created_at TIMESTAMP
);

---

## 11. Security

_What data in this system is sensitive? How do you protect it at rest and in transit?_

Sensitive data includes:
1. transcripts,
2. recordings,
3. customer information,
4. analysis results.

Protection Methods
At Rest:
1. encrypted database storage,
2. encrypted object storage.
In Transit:
HTTPS/TLS communication.
Access Control:
1. RBAC permissions,
2. audit logging,
3. customer isolation.
Logging:
Sensitive transcript data should not be logged directly.

---

## 12. API Interface

_Did you change the API contract (`POST /session/.../end`)? If yes, explain why. If no, explain why you kept it._

The external API contract remains unchanged:
POST /session/{id}/end
Reason:
1. avoids breaking existing integrations,
2. easier deployment,
3. simpler migration.
Only internal processing flow changes.

Webhook response:
{
  "status": "accepted"
}
This response is sent after saving the event successfully.


---

## 13. Trade-offs & Alternatives Considered

| Option                  | Why Considered         | Why Rejected                         |
| ----------------------- | ---------------------- | ------------------------------------ |
| Kafka                   | Durable streaming      | Too complex for current requirements |
| Redis only              | Fast queues            | Not durable enough                   |
| Direct async processing | Simple implementation  | Causes overload during bursts        |
| Autoscaling workers     | More processing power  | Does not solve TPM bottleneck        |
| Microservices           | Scalability            | Adds unnecessary complexity          |
| Postgres + Scheduler    | Durable and controlled | Final choice                         |


---

## 14. Known Weaknesses

_What are the gaps in your design? What would you address next?_
1. Scheduler becomes an important central component and needs monitoring.
2. Token estimation may not always be accurate.
3. Large bursts may still increase processing delay.
4. Polling recording readiness still depends on external provider reliability.
5. Very large customers may eventually require isolated infrastructure.
6. Retry queue entries are currently enqueued but no dedicated retry consumer/scheduler exists yet to replay failed jobs automatically.
---

## 15. What I Would Do With More Time

_Specific, prioritised list — not a generic wishlist._

1. Add better token prediction using historical data.
2. Build dashboard for queue monitoring.
3. Add distributed tracing.
4. Improve automatic retry tuning.
5. Add multi-region disaster recovery.
6. Build customer-facing processing status dashboard.
7. Add smarter workload prediction during peak hours.
8. Implement atomic retry dequeue using Redis Lua scripts or BLMOVE to prevent duplicate retry processing across workers.
