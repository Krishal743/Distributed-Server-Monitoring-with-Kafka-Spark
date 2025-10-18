# Real-time Distributed Server Monitoring Pipeline with Kafka and Spark

![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

A distributed, real-time pipeline designed to ingest, stream, and analyze server performance metrics using Apache Kafka and Apache Spark. The system identifies performance anomalies based on windowed aggregations.

---

## 📋 Table of Contents
- [Project Objective](#-project-objective)
- [System Architecture](#-system-architecture)
- [Core Technologies](#-core-technologies)
- [Project Structure](#-project-structure)
- [Setup and Installation](#-setup-and-installation)
- [How to Run](#-how-to-run)
- [Output Schema](#-output-schema)

---

## 🎯 Project Objective

The primary goal is to build a real-time distributed monitoring pipeline that can collect server metrics (CPU, Memory, Network, Disk), stream them through Kafka, and process them using Spark to identify abnormal patterns through window-based computations.

---

## 🏗️ System Architecture

The pipeline consists of four distinct roles operating on separate machines, interconnected via a ZeroTier virtual network.

**Data Flow:**
1.  **Producer:** Reads a simulated server metrics dataset and publishes records to four different Kafka topics.
2.  **Kafka Broker:** Manages four topics: `topic-cpu`, `topic-mem`, `topic-net`, and `topic-disk`.
3.  **Consumers:** Two independent consumers subscribe to their designated topics and write the raw data to local CSV files.
4.  **Spark Jobs:** After data ingestion, Spark jobs run on the consumer machines to perform window-based analysis and generate alert reports.

---

## 🛠️ Core Technologies

* **Apache Kafka:** Used as the distributed, high-throughput message bus for real-time data streaming.
* **Apache Spark:** Used for processing the collected data in batches, performing window-based aggregations and anomaly detection.
* **ZeroTier:** Provides a virtual network to ensure seamless and secure machine-to-machine communication across different environments.
* **Python:** The primary language for producer/consumer scripts and Spark jobs.

---

## 🚀 Setup and Installation

1.  **Prerequisites:**
    * Python 3.8+
    * Java 8 or 11
    * Working installations of Apache Kafka and Apache Spark.

2.  **Clone the Repository:**
    ```bash
    git https://github.com/Krishal743/Distributed-Server-Monitoring-with-Kafka-Spark.git
    cd     Distributed-Server-Monitoring-with-Kafka-Spark
    ```

3.  **Set up Python Environment:**
    It is highly recommended to use a virtual environment.
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

4.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

5.  **Network Configuration:**
    * Install ZeroTier on all four machines.
    * Create a network on the ZeroTier dashboard and have all machines join it.
    * Authorize all machines and note their assigned ZeroTier IP addresses.

---

## ▶️ How to Run

Execute the steps in the following order:

1.  **Broker Machine:**
    * Run the ZooKeeper and Kafka server startup commands.
    * Run the commands to create the four Kafka topics (`topic-cpu`, `topic-mem`, `topic-net`, `topic-disk`).
    * *See `scripts/broker_commands.txt` for details.*

2.  **Consumer Machines:**
    * On Consumer 1, run: `python scripts/consumer1_cpu_mem.py`
    * On Consumer 2, run: `python scripts/consumer2_net_disk.py`
    * Wait for them to show the "waiting for messages..." status.

3.  **Producer Machine:**
    * Once consumers are ready, run: `python scripts/producer.py`
    * Wait for the script to finish sending all data.

4.  **Stop Consumers:**
    * Press `Ctrl+C` on both consumer terminals to stop the ingestion scripts.

5.  **Run Spark Jobs:**
    * On Consumer 1, run: `spark-submit scripts/spark_job_1.py`
    * On Consumer 2, run: `spark-submit scripts/spark_job_2.py`
    * The final alert CSVs will be generated in the project's root directory.

---

## 📊 Output Schema

The final output consists of two CSV files containing identified anomalies.

**1. `team_47_CPU_MEM.csv`**
* **Schema:** `server_id, window_start, window_end, avg_cpu, avg_mem, alert`

**2. `team_47_NET_DISK.csv`**
* **Schema:** `server_id, window_start, window_end, max_net_in, max_disk_io, alert`

The `alert` column is populated only when a performance threshold is breached.
