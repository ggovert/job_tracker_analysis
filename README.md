Still on Progress


### Hacker News Job Tracker
An automated end-to-end data engineering pipeline that extracts, processes, and visualizes real-time hiring trends from Hacker News job postings.

### Objective: 
To build a production-grade, cost-effective data platform that automates the transition from unstructured social posts to structured market insights. I wanted to demonstrate how to orchestrate a modern data stack using open-source tools to monitor the tech job market.

### Process: 
- Orchestration: Scheduled Apache Airflow DAGs to monitor and trigger extractions whenever new "Who is Hiring" threads are posted.
- AI-Driven Extraction: Utilized LLMs to parse unstructured text into structured data (role, salary, location, tech stack).
- Storage & Querying: Implemented a Medallion Architecture using RustFS for object storage (Bronze/Silver layers) and
- DuckDB for lightning-fast analytical querying of the refined data.
- Deployment: Containerized the entire environment using Docker to ensure seamless deployment and environment consistency.
- Visualization: Built an interactive Streamlit dashboard to filter and visualize hiring trends and skill demands.

### Tools
- Orchestration: Apache Airflow
- Database & Storage: DuckDB, RustFS (Object Storage)
- AI/LLM: OpenAI/Local LLM for data extraction
- Frontend: Streamlit
- DevOps: Docker

### Value Proposition
This project showcases my ability to engineer automated, end-to-end data systems using the latest high-performance tools (DuckDB/RustFS). It proves I can handle the entire data lifecycle—from ingestion and AI-parsing to storage and visualization—while maintaining a focus on cost-efficiency and scalable deployment via Docker.
