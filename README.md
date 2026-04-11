# PipeForge

**PipeForge** is a utility for replicating data from sources to data warehouses. Built with PySpark, it provides a robust framework for extracting, loading, and transforming data efficiently.

## Features

- **PySpark-based processing**: Leverage distributed computing for large-scale data operations
- **Data warehouse integration**: Built-in support for loading data into data warehouses
- **Checkpoint management**: Track and manage data pipeline checkpoints
- **AWS integration**: Native support for AWS services (S3, etc.) via boto3
- **Database connectivity**: PostgreSQL support for source and target databases
- **Secret management**: HashiCorp Vault integration for secure credential handling
- **Extensible architecture**: Modular design with custom scripts and test support

## Installation

### Prerequisites

- Python 3.11 (< 3.12)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/payamy/PipeForge.git
cd PipeForge
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install the package:
```bash
pip install -e .
```

## Requirements

The project depends on:

- **pyspark** (3.5.2) - Distributed data processing
- **delta-spark** (3.0.0) - Delta Lake support for ACID transactions
- **pandas** (2.2.1) - Data manipulation
- **numpy** (1.26.2) - Numerical computing
- **psycopg2-binary** (2.9.9) - PostgreSQL adapter
- **boto3** (1.35.45) - AWS SDK for Python
- **hvac** (2.0.0) - HashiCorp Vault client
- **requests** (2.31.0) - HTTP library
- **urllib3** (2.2.3) - HTTP utilities

## Project Structure

```
PipeForge/
├── pipeforge/                 # Main package
├── pipeforge_checkpoint/      # Checkpoint management module
├── scripts/                   # Utility scripts
├── test/                      # Test suite
├── setup.py                   # Package configuration
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Usage

For detailed usage instructions and examples, refer to the documentation in the `scripts/` and `test/` directories.

## Contributing

Contributions are welcome! Please ensure your changes pass the test suite before submitting a pull request.

## License

This project is open source and available under the terms specified in the repository.

## Author

**payamy** - [GitHub Profile](https://github.com/payamy)