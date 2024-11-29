# Lupus Data Pipeline using Kedro framework

## Overview
This project processes and validates medical datasets related to Lupus patients. The pipeline cleans, merges, and validates data to produce a unified dataset.

## Requirements
- Python 3.8 or higher
- Kedro 0.19.10
- Dependencies listed in `requirements.txt`

1. Clone the repository:
   ```bash
   git clone https://github.com/karthikshankar98/Century-Health-Data-Engineering.git
   cd Century-Health-Data-Engineering

2. Create a virtual environment:
   ```bash
   python -m venv env
   env\Scripts\activate

3. Install dependencies:
   ```bash
   pip install -r requirements.txt

4. Run the pipeline from the project root directory:
   ```bash
   kedro run

## Key Files
nodes.py: Contains data cleaning and transformation logic.
pipeline.py: Defines the Kedro pipeline structure.
validation.py: Implements validation checks using Great Expectations.

