# 🏎️ Real-Time Data Streaming System  
### Distributed Systems 2025/2026 — MEI - Group 5

A real-time data streaming platform built with microservices and orchestrated using **Kubernetes**.  
The system simulates, processes, and visualizes live race data through a modular architecture that includes a **Producer**, **Consumer/API**, and **Web UI**, all continuously deployed using **GitHub Actions** and **Argo CD**.

---

## 🎯 Objectives

- Design and implement a **distributed system** with microservices architecture.  
- Deploy and manage services with **Kubernetes** (local and remote clusters).  
- Implement a **CI/CD pipeline** using **GitHub Actions** and **Argo CD**.  
- Enable **real-time data streaming** (via WebSockets or SSE).  
- Monitor system performance using **Prometheus** and **Grafana**.  
- Achieve scalability, low latency, and fault tolerance.

---

## ⚙️ System Overview

### Architecture
The project is divided into three main services:

#### 🧩 1. Producer (Data Generator)
- Simulates multiple race events in real time.  
- Publishes position, velocity, and participant updates to a **message broker** (e.g., Kafka, RabbitMQ, or Redpanda).  
- Configurable number of races, participants, and update intervals.

#### 🔧 2. Consumer (Processor & API)
- Subscribes to data topics from the broker.  
- Processes and optionally stores both raw and processed data.  
- Exposes a **REST API** for the UI and external components (e.g., ML modules).

#### 🖥️ 3. Web UI
- Displays an interactive **map** with all race participants in real time.  
- Provides **filters** and a **leaderboard** view.  
- Connects via REST API and optionally WebSocket/SSE for live updates.

---

## 🏗️ Technologies Used

| Area | Tools |
|------|-------|
| Containerization | Docker |
| Orchestration | Kubernetes / K3s |
| Messaging | RabbitMQ / Redpanda / Kafka |
| CI/CD | GitHub Actions + Argo CD |
| Monitoring | Prometheus + Grafana |
| Frontend | React / Vue / JavaScript-based UI |
| Backend | Python / Node.js (depending on team choice) |
| Data Storage | PostgreSQL / MongoDB (configurable) |

---

## 🔁 CI/CD Workflow

### GitHub Actions
- Builds Docker images for each service.  
- Pushes images to DockerHub.  

### Argo CD
- Automatically syncs the latest version of each service to the Kubernetes cluster.  
- Ensures declarative, version-controlled deployments.

---

## 📊 Monitoring

- Each service exposes Prometheus-compatible metrics.  
- Dashboards in Grafana visualize:
  - Message throughput
  - API latency
  - CPU/memory usage
  - Service uptime

---

## 🧠 Design Goals

- **High availability**: services can restart and recover gracefully.  
- **Low latency**: <500ms end-to-end data delay target.  
- **Security**: authentication, authorization, and basic DDoS protection.  
- **Scalability**: automatic scaling via Kubernetes horizontal pod autoscaling.

---

## 🧪 Running Locally

```bash
# Clone the repository
git clone https://github.com/<your-org-or-username>/real-time-streaming-system.git
cd real-time-streaming-system

# Build Docker images
docker compose build

# Start local environment (with K3s or Docker Desktop)
kubectl apply -f k8s/

# Access web UI
http://localhost:<your-ui-port>
```

---

## 📈 Monitoring Dashboard

- Prometheus: `http://localhost:9090`  
- Grafana: `http://localhost:3000` (login: admin / admin)

---

## 🧩 Project Timeline

| Phase | Description | Deadline |
|--------|--------------|-----------|
| Phase 1 | CI/CD setup and basic simulation | 12 Nov 2025 |
| Phase 2 | Full implementation and monitoring | 5 Jan 2026 |
| Final Demo | Presentation & discussion | 9 Jan 2026 |

---

## 📚 Report Structure

1. Summary  
2. Introduction  
3. CI/CD Pipeline Description  
4. System Implementation & Design Choices  
5. Metrics & Testing  
6. Results  
7. Conclusions & Future Work  

---

## 👥 Team Members

| Name | Role | Responsibility |
|------|------|----------------|
| Pedro Jaques | - | - |
| Roberto Andrade | - | - |
| Manuel Freitas | - | - |

---

## 🧩 License

This project is developed for academic purposes under the Distributed Systems 2025/2026 course (MEI). 

---

## 🛰️ Acknowledgments

- Course materials and documentation from Moodle.  
- Techworld with Nana (YouTube) for Kubernetes and CI/CD guides.  
- *Building Microservices* — Sam Newman (2021).  
- *Cloud Native DevOps with Kubernetes* — Domingus & Arundel.  
