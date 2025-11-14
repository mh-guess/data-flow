# data-flow

ETL system for loading financial data from Tiingo API to AWS S3 using Prefect.

## Getting Started

This repository contains a simple Prefect example flow to help you learn the basics before building the full ETL system.

### Prerequisites

- Python 3.8+
- pip

### Installation

1. Clone the repository:
```bash
git clone https://github.com/mh-guess/data-flow.git
cd data-flow
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Example Flow

The `example_flow.py` script demonstrates basic Prefect concepts:

```bash
python example_flow.py
```

This will run a simple ETL flow that shows:
- Task definition with `@task` decorator
- Flow definition with `@flow` decorator
- Task dependencies and data passing
- Basic logging and monitoring

### Testing with Prefect UI

To see your flows in the Prefect UI:

1. Start the Prefect server:
```bash
prefect server start
```

2. In a new terminal, run your flow:
```bash
python example_flow.py
```

3. Open your browser to http://127.0.0.1:4200 to see the Prefect UI

### Deploying to Prefect Cloud

1. Create a free account at https://app.prefect.cloud

2. Login from CLI:
```bash
prefect cloud login
```

3. Configure credentials in Prefect Cloud (see [SETUP.md](SETUP.md) for detailed instructions)

4. Run your flow - it will automatically sync to Prefect Cloud

## Next Steps

To build the full ETL system:

1. **Tiingo API Integration**: Uncomment `requests` in `requirements.txt` and add API calls
2. **AWS S3 Integration**: Uncomment `boto3` and `prefect-aws` in `requirements.txt`
3. **Scheduling**: Add deployment configuration for scheduled runs
4. **Error Handling**: Add retry logic and failure notifications
5. **Data Validation**: Add data quality checks

## Project Structure

```
data-flow/
├── example_flow.py      # Simple starter flow
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Resources

- [Prefect Documentation](https://docs.prefect.io)
- [Tiingo API Documentation](https://www.tiingo.com/documentation)
- [Prefect AWS Integration](https://prefecthq.github.io/prefect-aws/)
