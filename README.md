# LLM Post-Call Processing System

## Overview

This project is a backend system designed for scalable post-call processing using LLMs. It handles large-scale call data, processes transcripts, and extracts insights such as call outcomes, entities, and CRM updates.

The system is designed to handle high traffic (~100,000 calls per campaign) with reliability, prioritization, and rate-limit aware processing.

---

## Key Features

- Scalable post-call processing pipeline
- LLM-based transcript analysis
- Priority-based call handling (P0–P3)
- Retry mechanism with exponential backoff
- Redis-based queue system for async processing
- Correlation ID based request tracking
- Signal-based workflow execution
- Lead stage and CRM update support

---

## Architecture

Call End Event  
→ Stored in System  
→ Scheduled for Processing  
→ Worker picks task  
→ Recording fetched  
→ LLM analysis executed  
→ Results stored  
→ CRM update triggered (if enabled)

---

## Core Design Improvements

- Replaced fixed sleep-based recording wait with retry mechanism
- Improved reliability using structured retry logic
- Added better logging and traceability using correlation IDs
- Introduced queue-based asynchronous processing
- Separation of ingestion and processing layers

---

## Tech Stack

- Python
- FastAPI
- Celery
- Redis
- PostgreSQL (design level)
- AsyncIO

---

## Known Limitations

- Redis-based retry queue is not fully durable
- No centralized rate limit scheduler yet
- Token estimation is approximate
- Circuit breaker logic is coarse-grained

---

## Future Improvements

- Centralized LLM rate-limit scheduler
- Dead Letter Queue (DLQ) for failed tasks
- Distributed tracing (OpenTelemetry)
- Better token prediction system
- Real-time monitoring dashboard

---

## Author

Pooja Mohite