# Distributed Group Chat with Performance-Based Dynamic Load Balancer

A distributed real-time group chat application built using **Node.js, Socket.IO, PostgreSQL, and a Go-based performance-aware Load Balancer**.

The system is deployed across multiple backend servers. The Load Balancer dynamically selects a suitable backend based on backend health, CPU utilization, memory utilization, active requests, and request latency rather than using a fixed round-robin strategy.

The application provides:

- Real-time group chat using Socket.IO
- Persistent message storage using PostgreSQL
- Shared database across backend instances
- Cross-backend message synchronization
- Unique message IDs and duplicate prevention
- Digital signature verification
- AES-256-GCM encryption
- Backend health monitoring
- Performance-based dynamic load balancing
- CPU-threshold-based routing
- Runtime performance metrics
- Custom variable-load testing
- Response-time and utilization analysis

---

# 1. System Architecture

The deployed system consists of one public Load Balancer and three Node.js backend servers.

```text
                         CLIENTS
              ┌────────────┼────────────┐
              │            │            │
           User A       User B       User C
              │            │            │
              └────────────┼────────────┘
                           │
                    HTTP / Socket.IO
                           │
                           ▼
                ┌──────────────────────┐
                │    GO LOAD BALANCER  │
                │        Sys1          │
                │                      │
                │ Health Checking      │
                │ CPU Threshold        │
                │ Performance Score    │
                │ Dynamic Routing      │
                └──────────┬───────────┘
                           │
             ┌─────────────┼─────────────┐
             │             │             │
             ▼             ▼             ▼
        ┌─────────┐   ┌─────────┐   ┌─────────┐
        │  Sys2   │   │  Sys3   │   │  Sys4   │
        │ Node.js │   │ Node.js │   │ Node.js │
        │ Backend │   │ Backend │   │ Backend │
        └────┬────┘   └────┬────┘   └────┬────┘
             │             │             │
             └─────────────┼─────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │   PostgreSQL    │
                  │ Shared Storage  │
                  └─────────────────┘
