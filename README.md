# ARIS Parser · Data Loader · Driver Licence Comparator

## Overview
This project automates the **parsing, reporting, and uploading of driver licence data** from raw ARIS input files into the AssetWorks FleetFocus system. It was designed to support **government fleet management operations** by reducing manual intervention, improving reliability, and maintaining detailed audit logs.

⚠️ **Note on Sensitive Data**  
Since this project was built to handle **sensitive government information**, all **real data, credentials, and environment-specific configurations** have been **removed** from this repository.  
What remains here are the **calculation algorithms, workflow structures, comparison/reporting logic, and maintenance processes** — intended for **educational and reference purposes only.**

---

## Key Features
- **ARIS Parser**  
  Converts daily `.txt` input files into XML format compatible with AssetWorks (Excel 2003 XML schema).  
  Handles:
  - Licence number normalization and formatting  
  - Class + endorsement detection (e.g., Air Brake "Z")  
  - Field parsing: Driver Licence Number, Class, Expiry Date, Licence Status, Medical Due Date, Comments  

- **Daily Comparison Tool**  
  Compares today’s parsed XML with yesterday’s to identify **changes and alerts**, including:  
  - Licence status changes  
  - Class updates  
  - Expiring licences (within urgent 3-day window)  
  - Expiring medical due dates  
  - Errors and inconsistencies  

- **Automated Reporting**  
  - Generates a **main HTML summary report** (sent daily, even if no changes detected).  
  - Produces **individual operator email reports** when changes occur.  
  - Embeds formatted tables for clarity.  

- **Integration with FA Data Loader**  
  - Automatically generates and executes `.bat` files to upload the daily XML to AssetWorks.  
  - Confirms successful uploads by detecting generated `.txt` confirmation files.  

- **Robust Logging**  
  - Unified rolling log file with full run summary.  
  - Logs include total operators processed, changes detected, upload success/failure, and errors.  
  - Append-only log format with timestamps for audit trail.  

- **Self-Maintenance & Reliability**  
  - Creates required directories automatically if missing.  
  - Keeps **input backups** of original files with date-stamped copies.  
  - Cleans up stale XML and processed files before each new run.  

---

## Folder Structure
/DriverLicenceReports
│

├── AseetWorks Excel File # Stores generated XML files

├── comparison_reports # Main & individual HTML reports

│ └── emails # One-row operator email reports

├── DataLoad_21.1.x # FA Data Loader integration

│ └── logs # FA Data Loader raw log outputs

├── input # Raw ARIS input files

├── input backups # Archived daily ARIS input copies

├── logs # Unified application log (appends daily)

└── errors # Error snapshots


---

## Workflow
1. **Input Handling**  
   - Copies ARIS `.txt` input into `input backups/` before clearing `input/`  
   - Parses file → produces daily XML  

2. **Comparison & Reporting**  
   - Compares with yesterday’s XML  
   - Detects changes and expiry warnings  
   - Writes HTML reports  

3. **Upload to AssetWorks**  
   - Copies XML to `DataLoad_21.1.x`  
   - Auto-generates and runs `runfile.bat` for FA Data Loader  
   - Confirms via processed `.txt` file  

4. **Logging & Audit**  
   - Updates master log with results  
   - Appends FA Data Loader raw logs  

---

## Security & Data Disclaimer
- All sensitive data (operator names, licence numbers, server credentials, connection strings) has been **removed**.  
- The version shared here is a **technical framework** for workflow automation and comparison logic, **not production-ready**.  
- To adapt for real-world use, private credentials and environment-specific configurations must be provided separately.  

---

## Requirements
- **Python 3.13+**
- Libraries:
  - `pandas`
  - `openpyxl`
  - `lxml` (for XML handling)
  - `psutil` (process handling, optional for monitoring features)

---

## Usage
1. Place raw ARIS `.txt` file in `/input/`  
2. Run:
   ```bash
   python daily_driver_check.py
