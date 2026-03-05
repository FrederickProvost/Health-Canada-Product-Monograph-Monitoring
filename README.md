📌 Overview
This project provides an automated monitoring solution to track Product Monograph (PM) updates published by Health Canada through the Drug Product Database (DPD).
It combines:

a Python monitoring script
GitHub Actions for scheduled execution
Power Automate for conditional email alerting

The goal is to detect real PM updates only and notify users without generating unnecessary alerts.

🧠 How It Works
Python script
   ↓
CSV dataset (GitHub)
   ↓
Power Automate flow
   ↓
Email alert (only if changes detected)


🐍 Python Monitoring Script
The Python script:

Queries Health Canada DPD product pages
Extracts the latest PM update date
Compares it to the previously stored value
Flags changes using a boolean indicator


🤖 GitHub Actions
GitHub Actions are used to:

Run the Python script on a schedule
Regenerate the CSV dataset
Publish the updated data to the repository

This allows external tools (e.g. Power Automate) to consume the data easily.

🔔 Power Automate Alerting
A Power Automate flow:

Retrieves the CSV from GitHub
Parses and structures the data
Filters products where has_changed = 1
Sends an HTML email alert only if at least one change is detected

The email contains a clean, styled table with:

Product code
PM update date
Direct link to the Product Monograph


✅ Key Features

✅ End‑to‑end automation
✅ No false positives
✅ Noise‑free alerting
✅ Human‑readable HTML emails
✅ Easy to extend to more products


🚀 Use Cases

Regulatory monitoring
Compliance tracking
Pharmacovigilance support
Business Intelligence pipelines
Automation demonstrations


📌 Notes

This repository is intended as a technical demonstration
No confidential or proprietary data is included
The architecture can be adapted to other jurisdictions or data sources
