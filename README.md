# State of the Bet

A real-time prediction market data pipeline powered by Kafka and PostgreSQL.

## Overview

State of the Bet tracks political prediction markets, processing live betting odds and trends. The pipeline ingests data, stores it in PostgreSQL, and provides structured insights for analysis.

## Tech Stack

- Python – Core development
- Kafka – Streaming data ingestion
- PostgreSQL – Structured data storage
- FastAPI (Optional) – API for querying data
- Docker – Containerization for reproducibility

## Getting Started

### Clone the repository

```sh
git clone https://github.com/yourusername/StateOfTheBet.git
cd StateOfTheBet
```

### Install dependencies

```sh
pip install -r requirements.txt
```

### Run the Kafka producer

```sh
python src/producer.py
```

## License

MIT License
