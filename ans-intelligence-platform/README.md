# ANS Intelligence Platform

**An end-to-end data and machine learning platform for Brazilian healthcare insurance market intelligence.**

## Overview

ANS Intelligence Platform is a data-driven platform designed to collect, process, analyze, and extract insights from public datasets provided by the Brazilian National Supplementary Health Agency (Agência Nacional de Saúde Suplementar — ANS).

The project aims to build an integrated ecosystem combining Data Engineering, Machine Learning, Software Engineering, DevOps, and MLOps practices.

The platform will transform raw public data into reliable analytical assets, develop machine learning solutions for different business domains, and expose analytical capabilities through APIs and web applications.

**Project status:** Under development.

## Project Objectives

- Build reliable and scalable data ingestion and transformation pipelines.
- Establish a structured Data Lake architecture.
- Implement data quality, validation, and governance practices.
- Develop machine learning models for healthcare insurance market intelligence.
- Expose model predictions and analytical capabilities through APIs.
- Integrate independent services through a Java-based backend.
- Provide a web interface for data exploration and analytical insights.
- Apply DevOps, MLOps, and observability practices throughout the platform.

## Machine Learning Domains

The platform will initially focus on five analytical domains:

| Domain                | Description                                                                                   |
| --------------------- | --------------------------------------------------------------------------------------------- |
| Financial Risk        | Analyze financial indicators and identify potential risk patterns among healthcare operators. |
| Financial Forecast    | Forecast financial indicators and trends using historical data.                               |
| Beneficiary Analytics | Explore beneficiary distribution, composition, and historical trends.                         |
| Pricing Intelligence  | Analyze pricing patterns and relationships across the healthcare insurance market.            |
| Customer Intelligence | Identify customer profiles, behavioral patterns, and relevant analytical segments.            |

Each domain will evolve independently while sharing common data and machine learning infrastructure.

## High-Level Architecture

The platform follows a microservices-oriented architecture, separating data processing, machine learning, application services, and presentation layers.

### Data Platform

Responsible for data acquisition, ingestion, transformation, validation, and data quality.

### Data Lake

The Data Lake will use a medallion architecture:

- **Bronze:** Original source data and ingestion history.
- **Silver:** Cleaned, standardized, and integrated datasets.
- **Gold:** Analytical datasets, business indicators, and machine learning features.

MinIO and Parquet will be used for object storage and analytical data persistence.

### Machine Learning Platform

Responsible for experimentation, model training, evaluation, versioning, and lifecycle management.

### Inference Services

Independent Python services will expose approved machine learning models through APIs.

### Backend Platform

Java and Spring Boot will provide application-level integration, business logic, access management, and communication between platform services.

### API Gateway

A centralized entry point will manage access to backend and analytical services.

### Frontend

An Angular application will provide dashboards and user-facing analytical capabilities.

## Technology Stack

The following technologies are planned for the initial architecture:

| Area                | Technologies           |
| ------------------- | ---------------------- |
| Data Engineering    | Python, SQL, PySpark   |
| Data Transformation | dbt                    |
| Data Storage        | MinIO, Parquet         |
| Relational Database | PostgreSQL             |
| Machine Learning    | Scikit-learn           |
| Experiment Tracking | MLflow                 |
| Model Serving       | FastAPI                |
| Backend             | Java, Spring Boot      |
| Frontend            | Angular                |
| Infrastructure      | Docker, Docker Compose |
| Version Control     | Git                    |
| CI/CD               | GitHub Actions         |
| Monitoring          | Prometheus, Grafana    |

The technology stack may evolve as architectural requirements become clearer.

## Repository Structure

```text
ans-intelligence-platform/
├── backend/
├── data-platform/
├── docs/
├── front-end/
├── infrastructure/
├── ml-platform/
├── shared/
├── tests/
├── .env.example
├── .gitignore
├── docker-compose.yml
└── README.md
```

## Development Roadmap

The project will be developed incrementally through the following phases:

1. **Foundation & Infrastructure** — Repository structure, development environment, containerization, and core infrastructure.
2. **Data Engineering Platform** — Data acquisition, ingestion, transformation, quality, and storage.
3. **Machine Learning Foundation** — Experiment tracking, training workflows, model evaluation, and versioning.
4. **Machine Learning Domains** — Implementation of the five analytical domains.
5. **Model Serving** — Deployment and exposure of trained models through APIs.
6. **Java Integration Platform** — Backend services, application integration, and API Gateway.
7. **Frontend** — Angular dashboards and analytical interfaces.
8. **DevOps, MLOps & Observability** — Automation, monitoring, testing, and lifecycle management.

## Data Sources

The initial data source will be the Brazilian National Supplementary Health Agency (ANS) Open Data Portal.

Official source:

https://dadosabertos.ans.gov.br/FTP/PDA/

All ingestion processes will be designed to preserve source traceability and support reproducibility.

## Engineering Principles

The platform will follow these principles:

- Modular and independently maintainable services.
- Reproducible data and machine learning workflows.
- Data quality and lineage as first-class concerns.
- Clear separation between training and inference.
- API-driven service integration.
- Infrastructure automation and containerization.
- Incremental architectural evolution based on actual requirements.

## Project Scope

This is an engineering and learning project focused on building a complete data and machine learning ecosystem using public datasets.

Analytical outputs and predictive models are experimental and should not be interpreted as official ANS assessments or financial recommendations.

---

**ANS Intelligence Platform — Turning public healthcare data into actionable intelligence.**
