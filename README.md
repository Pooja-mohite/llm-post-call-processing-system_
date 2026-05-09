# llm-post-call-processing-system_
Scalable LLM-based post-call processing system with priority scheduling, retry handling, and rate-limit aware orchestration for high-volume call analytics.
# LLM Post-Call Processing System

## Overview
This project is a scalable backend system designed for post-call processing using LLMs. It handles high-volume call data, applies intelligent prioritization, manages rate limits, and ensures reliable processing through retry mechanisms and async orchestration.

The system is designed with production-level considerations such as fault tolerance, queue management, and observability.

---

## Key Features

- Async post-call processing pipeline
- LLM-based call analysis
- Priority-based task handling (P0–P3)
- Retry mechanism with exponential backoff
- Redis-based queue management
- Correlation ID tracking for debugging
- Signal-based workflow triggering
- Lead stage updates after processing
- Rate-limit aware processing design

---

## Architecture Highlights

- Call End Event → Stored → Scheduled → Processed by Worker
- Centralized scheduling approach for load control
- Separation of ingestion and processing layers
- Parallel execution of recording fetch + LLM analysis
- Retry queue for failed tasks

---

## Tech Stack

- Python
- Celery
- Redis
- AsyncIO
- PostgreSQL (design-level)
- LLM API integration

---

## Design Principles

- High scalability for ~100K calls/day
- Failure isolation per task
- No data loss guarantee for completed calls
- Backpressure handling using queues
- Observability via structured logs & metrics

---

## Improvements Implemented

- Replaced fixed sleep-based logic with retry-based recording fetch
- Parallelized recording upload and LLM processing
- Improved retry queue with exponential backoff
- Added correlation ID for traceability
- Enhanced logging and metrics tracking

---

## Known Limitations

- Redis-based retry queue is not fully durable
- Scheduler is not yet centralized
- Token estimation is approximate
- DLQ is not fully automated

---

## Future Enhancements

- Central scheduling system for rate control
- Dead Letter Queue (DLQ) automation
- Distributed tracing (OpenTelemetry)
- Better token prediction model
- Dashboard for queue and failure monitoring

---

## Author
Pooja Mohite
