# README

## Overview

This project tracks trading flows on Binance by aggregating trade data from the WebSocket API and storing it in a PostgreSQL database. The project includes several key components:

- **WebSocket Connection**: Subscribes to aggregated trade events for specified trading pairs.
- **Database Operations**: Inserts aggregated trade data into a PostgreSQL table and periodically cleans up old records.
- **Debugging**: Logs useful information for debugging purposes.

## Usage

### Prerequisites

- Go installed (`>= 1.24`)
- PostgreSQL server running
- Environment variables set:
  - `POSTGRES_DSN`: Database connection string.
  - `PAIRS_OVERRIDE`: Comma-separated list of trading pairs to monitor.
  - `DEBUG`: Set to `1` to enable debug logging.
  - `NETWORK_NAME="database-network"` As default network.

### Running the Project

1. Clone the repository:

   ```sh
   git clone https://github.com/ricgrangeia/go-trades-market-track.git
