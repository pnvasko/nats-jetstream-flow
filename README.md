# nats-jetstream-flow

[![Build Status](https://github.com/pnvasko/nats-jetstream-flow/actions/workflows/go.yml/badge.svg)](https://github.com/pnvasko/nats-jetstream-flow/actions/workflows/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/pnvasko/nats-jetstream-flow.svg)](https://pkg.go.dev/github.com/pnvasko/nats-jetstream-flow)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A Go toolkit for building reliable distributed workflows, state management, and coordination primitives leveraging **NATS JetStream** Key-Value and Messaging capabilities.

## What is it?

`nats-jetstream-flow` provides opinionated building blocks to simplify the development of distributed applications that rely on NATS JetStream for communication and persistent state. It abstracts common patterns like:

-   **Distributed Shared State:** Managing application state accessible and writable by multiple distributed instances using JetStream Key-Value.
-   **Optimistic Concurrency Control:** Handling concurrent updates to shared state using Read-Modify-Write (RMW) cycles with automatic retries (`ErrWrongLastSequence`).
-   **Workflow Orchestration:** Processing messages in a flow-like manner, coordinating state updates across distributed workers.
-   **Coordination Primitives:** Building higher-level concepts like distributed rate limiters or task state management on top of reliable state.
-   **Observability:** Integrating tracing and logging for better insight into distributed operations.

## Inspiration
This project's approach to building data flows and stateful processing pipelines is inspired by the concepts found in stream processing libraries, particularly [go-streams](https://github.com/reugn/go-streams). While `nats-jetstream-flow` is specifically tailored for the NATS JetStream ecosystem and focuses on integrating persistent KV state, the fundamental idea of composing operations on data flowing through a system draws from similar paradigms.

## Why use it?

If you are building a distributed system in Go and are using NATS JetStream, this library can help you:

-   **Reduce Boilerplate:** Provides ready-to-use patterns for common distributed system challenges like state management and concurrency.
-   **Increase Reliability:** Built-in RMW retries and context-aware operations help handle transient failures and concurrent updates.
-   **Improve Structure:** Encourages modular design through interfaces (like `Object`, `MessageHandler`).
-   **Simplify Observability:** Standardized logging and tracing integration across components.

## Features

-   **`coordination/ObjectStore`:** A generic, type-safe store for managing Protobuf-serializable objects in JetStream KV.
    -   Optimistic concurrency (RMW) with retry logic.
    -   Per-key synchronization within a single process instance to reduce internal contention.
    -   Context-aware operations (`Update`, `Read`, `Reset`, `Watch`, `Delete`).
-   **Reusable Primitives:** Concrete implementations built on `ObjectStore` for common coordination tasks (e.g., Rate Limiting in `pkg/rate_limit_object`).
-   **`handlers/MessageHandler`:** Interface for defining message processing steps in a flow.
-   **`handlers/FlowHandler`:** An orchestrator for processing messages through a sequence of `MessageHandler` steps, managing task state in JetStream KV.
-   **`common` Package:** Standardized setup for:
    -   Configuration loading (e.g., from environment variables).
    -   Structured logging (e.g., using Zap, integrated with context).
    -   Distributed tracing (using OpenTelemetry).
-   **`proto` Package:** Protobuf definitions for efficient data serialization and versioning.
-   **Comprehensive Examples:** Demonstrations covering distributed state, metrics collection, and workflow processing (`examples/`).


## Architecture Highlights

The library's core revolves around a few key concepts:

1.  **`Object` Interface:** Represents any application-specific data structure that needs to be stored and managed in JetStream KV. It defines methods for labeling (mapping application parameters to a unique KV key), serialization (`MarshalVT`, `UnmarshalVT`), and updating (`Update`).
2.  **`ObjectStore<T, R>`:** The generic KV abstraction. It takes an implementation of the `Object` interface (`T`) and an input type for updates (`R`). It handles the low-level JetStream KV interactions, including RMW, retries, and per-key locking.
3.  **`MessageHandler` / `FlowHandler`:** These components consume NATS messages and orchestrate application logic. They often interact with `ObjectStore` instances to read and update shared state as part of message processing, enabling distributed workflows.

Protobuf (`proto`) and the `common` package for observability are utilized across these layers to ensure consistency and efficiency.

## Getting Started

### Prerequisites

-   Go (1.18 or higher)
-   A running NATS server with JetStream enabled.

### Installation

```bash
go get github.com/pnvasko/nats-jetstream-flow
```

## Examples
Explore the examples/ directory for more comprehensive usage patterns, including:
- Implementing different types of distributed metrics (metrics_collection).
- Building and running a distributed workflow with task state management (flow_handler, task_store).
- Setting up NATS connections, logging, and tracing using the common package.

## License
This project is licensed under the MIT License - see the LICENSE file for details.
