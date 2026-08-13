# Real-Time F1 Telemetry Pipeline

This repository contains an end-to-end telemetry simulation and real-time inference engine designed for Formula 1 data. It simulates high-frequency data streams, evaluates machine learning models on the fly, aggregates race statistics, and surfaces live insights via a WebSocket dashboard and a LibreChat-powered AI assistant.

## Architecture

The pipeline consists of four main stages:

1. **Java Producer (CSV → Kafka)**
   Simulates real-time telemetry by streaming historical per-driver CSV data into a Kafka topic (`telemetryf1test`) at configurable playback speeds.
2. **Kafka → ClickHouse**
   Kafka streams are ingested into ClickHouse's high-performance `raw_telemetry` table using a Kafka Engine + Materialized View setup.
3. **Inference Engine (Python)**
   Polls ClickHouse for the latest telemetry, executes machine learning models (XGBoost/scikit-learn) to predict pit stops, tire degradation, and win probabilities, and writes the results back to ClickHouse (`prediction_results`).
4. **Dashboard & AI Agent**
   - **FastAPI / UI**: Serves a real-time WebSocket dashboard that polls the ClickHouse aggregate views and visualizes live race telemetry.
   - **LibreChat & MCP**: An LLM agent (equipped with the ClickHouse Model Context Protocol) can query the real-time database to answer complex questions about the ongoing race strategy.

## Prerequisites

- **Docker** and **Docker Compose**
- **PowerShell** (for Windows users running the orchestrator script)

*Note: The Java telemetry producer is containerized using a multi-stage Docker build, so a local JDK/Maven installation is not strictly required.*

## Quickstart

1. **Clone the repository**
2. **Setup environment variables**
   ```bash
   cp .env.example .env
   # Edit .env to add your GOOGLE_KEY or other LLM keys for LibreChat
   ```
3. **Run the End-to-End Script (Windows)**
   A PowerShell orchestrator script is provided to spin up the entire stack, compile the producer, launch ClickHouse, Kafka, LibreChat, and the UI:
   ```powershell
   cd telemetry-producer
   .\run_end_to_end.ps1 -EventName "Miami_Grand_Prix" -RaceYear 2026 -Speed 10
   ```
   *Note: This script takes several minutes on first run to download Docker images and compile the Java producer.*

4. **Access the Services**
   - **Real-Time Dashboard**: `http://localhost:8000`
   - **LibreChat AI Interface**: `http://localhost:3080`
   - **ClickHouse DB**: `localhost:8123` (default user, no password)

## Configuration

You can change the target race by passing arguments to the orchestrator script:
- `-EventName "Japanese_Grand_Prix"`
- `-RaceYear 2026`
- `-Speed 15` (multiplier for real-time telemetry playback)

Alternatively, these can be set as standard environment variables (`EVENT`, `YEAR`, `SPEED_FACTOR`) if running `docker-compose up` manually.

## Directory Structure

- `telemetry-producer/` - Java Kafka Producer & Python Inference Engine
- `ui/` - FastAPI backend and Vanilla JS/HTML/CSS dashboard
- `models/` - Pre-trained ML models for predictions
- `udf_config/` - ClickHouse User Defined Functions configuration
- `docker-compose.yml` - Unified infrastructure stack (Kafka, Zookeeper, ClickHouse, LibreChat, Mongo, Meilisearch)
