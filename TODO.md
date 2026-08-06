# DEBUG: Trace CSV Parsing Pipeline (Empty Line Items)

## Goal
Trace where the new CSV data format breaks parsing/mapping before implementing validation blocks.

## Steps
- [x] Add verbose logging: Raw Ingestion (raw row from CSV reader)
- [x] Add verbose logging: Normalization/Mapping (header + field transforms)
- [x] Add verbose logging: Pre-Build State (variables into build_lines / find_or_create_product)
- [x] Review logs together to identify mapping breakdown
- [x] Implement Zero-Dollar Line Policy in build_lines() (app.py)
  - [x] Split & Extract: comma-separated Product / Service into distinct items
  - [x] Zero-Dollar Itemization: $0.00 line per extracted item
  - [x] Total Value Line: append "Total Visit Charges" with real total
- [x] Recompile / import-check app.py
- [x] Zero-Dollar refinement: pass item name into ItemRef.name, omit outer Description
- [x] Add QuickBooksAuthError handling in process_csv_file (auth gate safety net)
- [x] Part 1: Session-based auth gate (redirect to /login before dashboard)
  - [x] Set session['connected'] in /callback
  - [x] require_connection guard
  - [x] Gate / and /upload
  - [x] Gate /status (optional)
- [x] Part 2: Token-age audit in src/qb_auth.py
  - [x] Record qb_refresh_token_issued_at on mint
  - [x] Add refresh_token_health() method
  - [x] Log token age on refresh
