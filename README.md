# Health Canada – Product Monograph Monitor

This project monitors Product Monograph / Veterinary Labelling dates
from the Health Canada Drug Product Database (DPD).

## What it does
- Reads a list of Drug_code values from Excel
- Fetches PM dates from Health Canada
- Detects changes compared to the previous run
- Sends an email alert if changes are detected

## How it runs
- Designed to run daily via automation (GitHub Actions / Task Scheduler)
