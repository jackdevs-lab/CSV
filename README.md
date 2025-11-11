# CSV → QuickBooks Integration

A small Flask-based utility that parses CSV exports (invoices/receipts), maps transactions and customers, and creates corresponding records in QuickBooks via the QuickBooks API.

This repository contains a CSV parser, mapping logic, services for interacting with QuickBooks (customers, products, invoices, receipts), and a simple web UI to upload files.

## Table of Contents

- Overview
- Features
- Prerequisites
- Installation
- Configuration
- Usage
- Project structure
- Running tests
- Troubleshooting
- Contributing
- License

## Overview

The app reads CSV files matching the expected schema, groups rows by invoice number, maps patients/customers and products, and posts invoices or sales receipts to QuickBooks. It includes basic validation and logging and moves processed files into `data/processed/` or `data/error/` depending on outcome.

## Features

- CSV parsing and validation
- Transaction mapping (invoice vs receipt)
- Create or find customers and products in QuickBooks
- Support for markup rules (insurance/pharmacy/lab/radiology)
- Simple web UI to upload CSV files

## Prerequisites

- Python 3.10+ 
- pip
- A QuickBooks developer account and API credentials (stored in `.env`)

## Installation

1. Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

3. (Optional) Run tests to verify the environment:

```powershell
vercel dev 
```

## Configuration

- `config/mappings.json` — product/category mappings used by the mapper.
- `.env` — QuickBooks API tokens and credentials. Keep this file secure and do not commit secrets to source control.
- `config/settings.py` — basic app settings.

Make sure `.env` is populated before attempting to process CSVs that will communicate with QuickBooks.

## Usage

Start the Flask app (development mode):

```powershell
python src/main.py
```

Open a browser at http://127.0.0.1:5000/ to use the upload UI.

API endpoint (for programmatic uploads): POST a multipart/form-data request with a `file` field to `/upload`. The endpoint returns JSON with `success` and `logs`.

Processing behavior:
- The CSV parser validates required columns. Missing required columns are logged and the file is moved to `data/error/`.
- Successfully processed files are moved to `data/processed/`.

## Project structure

Key files and directories:

- `src/` — application source code
	- `main.py` — Flask app and processing workflow
	- `csv_parser.py` — CSV parsing utilities
	- `mapper.py` — transaction mapping rules
	- `*_service.py` — service classes for QuickBooks actions
	- `qb_auth.py`, `qb_client.py` — QuickBooks authentication and HTTP client
- `config/` — configuration and token files
- `data/` — sample input, processed and error files
- `tests/` — unit tests (run with pytest)

## Running tests

Run the test suite with pytest:

```powershell
python -m pytest -q
```

The tests in `tests/` include unit tests for the CSV parser, mapper, and QuickBooks client abstractions.

## Troubleshooting

- If uploads fail with authentication errors, verify `config/qb_tokens.json` and your QuickBooks app settings.
- Check the web UI logs displayed after upload for specific warnings or errors.
- Ensure CSV files include the required columns:
	- `Invoice No.`, `Patient Name`, `Patient ID`, `Product / Service`, `Description`, `Total Amount`, `Quantity`, `Unit Cost`, `Service Date`, `Mode of Payment`

#



