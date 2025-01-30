<div align="center">
  <h1>SCORE UPDATER SERVICE</h1>
</div>

## Overview
The Score Updater Service is a real-time service designed to handle score update from client and consume from kafka broker.  

The service is built using:
- **Golang** for development
- **Gin** for HTTP routing
- **Gorilla WebSocket** for real-time communication
- **PostgreSQL with pgx** for data persistence
- **JWT** for authentication
- **Testify** for testing
---

## Architecture

The architecture is divided into three main layers:

### **1. Transport Layer**
- Handles incoming requests, including HTTP, WebSocket, and gRPC.
- Manages request routing, authentication (JWT), access control, and parameter validation.
- Uses **Gin** for REST API routing and **Gorilla WebSocket** for real-time communication.

### **2. Service Layer**
- Implements the core business logic and use cases.
- Processes score updates, publishes events to Kafka, and manages real-time WebSocket communication.

### **3. Repository Layer**
- Handles data persistence and external communication.
- Uses **pgx** to interact with PostgreSQL for database operations.
- Integrates with **Kafka** for event-driven messaging.

---

## Tech Stack
- **Programming Language**: Golang
- **Web Framework**: Gin
- **WebSocket Library**: Gorilla WebSocket
- **Database**: PostgreSQL with pgx
- **Message Broker**: Kafka

---

## Features
- **Real-Time Score Updates**: Publish score updates to connected clients via WebSocket.
- **REST API**: Manage scores using RESTful endpoints.
- **Event-Driven Architecture**: Use Kafka for asynchronous event handling.

---

## Getting Started

### **Prerequisites**
- Go 1.22.5
- PostgreSQL
- Kafka

---

### **Installation**

#### 1. Clone the repository:
```bash ```
git clone https://github.com/nirmal100prk/score-updater-service.git
cd score-updater-service

#### 2. Install dependencies:
go mod download

#### 3. Setup env variable:

#### 4. Run the service:
go run ./cmd/main.go