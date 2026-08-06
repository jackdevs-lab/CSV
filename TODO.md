# DEBUG: Trace CSV Parsing Pipeline (Empty Line Items)

## Goal
Trace where the new CSV data format breaks parsing/mapping before implementing validation blocks.

## Steps
- [x] Add verbose logging: Raw Ingestion (raw row from CSV reader)
- [x] Add verbose logging: Normalization/Mapping (header + field transforms)
- [x] Add verbose logging: Pre-Build State (variables into build_lines / find_or_create_product)
- [ ] Review logs together to identify mapping breakdown
- [ ] Fix extraction logic for new CSV format
- [ ] Then implement validation layers (deferred)
