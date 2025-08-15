import os
import sys
import re
import time
import smtplib
import shutil
import socket
import subprocess
import configparser
import pandas as pd
from html import escape
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from xml.dom import minidom
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# === CONFIG LOADER
def _app_dir():
    # location of the running script or frozen exe
    return os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__))

CONFIG_PATH = os.path.join(_app_dir(), "config.ini")

def load_config():
    cfg = configparser.ConfigParser()
    if not os.path.exists(CONFIG_PATH):
        # seed defaults from your current script
        cfg["EMAIL"] = {
            "from_address": "no-reply@northbay.ca",
            "recipients": "eri.mojdehi@northbay.ca"
        }
        cfg["PATHS"] = {"base_dir": r"C:\Users\erim\Desktop\DriverLicenceReports"}
        cfg["SERVER"] = {"host": "v-fleetfocustest", "port": "2000"}
        cfg["UPLOAD"] = {"fadataloader_user": "SYSADMIN-ARIS", "fadataloader_pass": "CNB4Lp5$Q1J5m"}
        cfg["POLICY"] = {"expiry_window_days": "7"}
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            cfg.write(f)
    else:
        cfg.read(CONFIG_PATH, encoding="utf-8")
    return cfg

cfg = load_config()

# normalize values/types
import re as _re
FROM_ADDRESS = cfg.get("EMAIL", "from_address", fallback="no-reply@northbay.ca")
_raw_rcpts = cfg.get("EMAIL", "recipients", fallback="eri.mojdehi@northbay.ca")
EMAIL_RECIPIENTS = [r.strip() for r in _re.split(r"[;,]", _raw_rcpts) if r.strip()]

BASE_DIR = cfg.get("PATHS", "base_dir", fallback=r"C:\Users\erim\Desktop\DriverLicenceReports")

SERVER_HOST = cfg.get("SERVER", "host", fallback="v-fleetfocustest")
SERVER_PORT = cfg.getint("SERVER", "port", fallback=2000)

FA_USER = cfg.get("UPLOAD", "fadataloader_user", fallback="SYSADMIN-ARIS")
FA_PASS = cfg.get("UPLOAD", "fadataloader_pass", fallback="CNB4Lp5$Q1J5m")

EXPIRY_WINDOW_DAYS = cfg.getint("POLICY", "expiry_window_days", fallback=7)

# === SETTINGS ===
# Define base directory and all subfolders used by the program for input, output, reports, logs, and reference files
FOLDERS = {
    "input": os.path.join(BASE_DIR, "input"),
    "output": os.path.join(BASE_DIR, "output"),
    "reports": os.path.join(BASE_DIR, "comparison_reports"),
    "logs": os.path.join(BASE_DIR, "logs"),
    "assets": os.path.join(BASE_DIR, "assets"),
    "emails": os.path.join(BASE_DIR, "comparison_reports", "Individual emails"),
    "data_loader": os.path.join(BASE_DIR, "DataLoad_21.1.x"), 
}

# Configuration: how many days before expiry should trigger a warning, and whether to delete the previous day's file
DELETE_YESTERDAY_OUTPUT = True

# Server check function
def is_server_online(host, port, timeout=3):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

# === DEFINE TODAY/YESTERDAY PATHS ===
# Establish filenames and paths for today’s input, output, logs, and report files
today = datetime.today().date()
yesterday = today - timedelta(days=1)
input_file = os.path.join(FOLDERS["input"], f"input_{today}.txt")
today_xml = os.path.join(FOLDERS["output"], f"ARIS_{today}.xml")
yesterday_xml = os.path.join(FOLDERS["output"], f"ARIS_{yesterday}.xml")
report_file = os.path.join(FOLDERS["reports"], f"comparison_{today}.html")
log_file = os.path.join(FOLDERS["logs"], f"driver_log_{today}.txt")
employee_csv = os.path.join(FOLDERS["assets"], "Active Operator List.csv")

# Keep only the 3 most recent log files
log_files = sorted(
    [f for f in os.listdir(FOLDERS["logs"]) if f.startswith("driver_log_") and f.endswith(".txt")],
    reverse=True
)
for old_log in log_files[:-3]:  # keep only 3 most recent
    try:
        os.remove(os.path.join(FOLDERS["logs"], old_log))
    except Exception as e:
        print(f"⚠️ Failed to delete old log: {old_log} – {e}")

# Custom logging function to timestamp messages and store them in memory
def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}")

# === EMAIL FUNCTION ===
def send_email_html(to_addresses, subject, html_content):
    from_address = FROM_ADDRESS

    if isinstance(to_addresses, str):
        to_addresses = [to_addresses]  # ensure list format

    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = ", ".join(to_addresses)
    msg['Subject'] = subject
    msg.attach(MIMEText(html_content, 'html'))

    try:
        with smtplib.SMTP("smtp.northbay.ca", 25) as server:
            server.starttls()
            server.send_message(msg)
            log(f"📧 Email sent to: {', '.join(to_addresses)}")
    except Exception as e:
        log(f"❌ Failed to send email to {', '.join(to_addresses)}: {e}")

# Access check
def check_directory_write_access(folder_paths):
    for folder in folder_paths:
        test_file = os.path.join(folder, ".__test_write.tmp")
        try:
            with open(test_file, "w") as f:
                f.write("test")
            os.remove(test_file)
        except Exception as e:
            log(f"❌ ERROR: Cannot write to folder: {folder}")
            log(f"   Reason: {e}")
            print(f"❌ Program aborted due to folder access error.")
            sys.exit(1)

# Load the employee master CSV and validate its format
def load_employee_csv():
    if not os.path.exists(employee_csv):
        log(f"❌ Employee CSV not found: {employee_csv}")
        return pd.DataFrame()

    # New schema: DepartmentID, DepartmentName, OperatorName, OperatorID, LicenceNo
    df = pd.read_csv(employee_csv)

    required = {"DepartmentID", "DepartmentName", "OperatorName", "OperatorID", "LicenceNo"}
    if not required.issubset(df.columns):
        raise ValueError(f"❌ Employee CSV missing required columns. Have: {list(df.columns)} | Need: {sorted(required)}")

    # Normalize licence numbers (remove dashes/spaces) — stored back in the SAME column
    df["LicenceNo"] = df["LicenceNo"].astype(str).str.replace("-", "").str.replace(" ", "")

    return df

# Utility to remove dashes and spaces from licence numbers for consistent matching
def normalize_Licence_number(val):
    return str(val).replace("-", "").replace(" ", "")

# === INIT DIRECTORIES ===
for path in FOLDERS.values():
    os.makedirs(path, exist_ok=True)

# === CHECK WRITE PERMISSIONS FOR ALL FOLDERS ===
check_directory_write_access([
    FOLDERS["input"],
    FOLDERS["output"],
    FOLDERS["logs"],
    FOLDERS["reports"],
    FOLDERS["emails"],
    FOLDERS["assets"],
    FOLDERS["data_loader"],
])

# === CLEAN UP OLD HTML REPORTS ===
for folder in [FOLDERS["reports"], FOLDERS["emails"]]:
    for f in os.listdir(folder):
        if f.endswith(".html"):
            try:
                os.remove(os.path.join(folder, f))
            except Exception as e:
                print(f"⚠️ Failed to delete old HTML report: {f} – {e}")

# === ARIS TEXT PARSER ===
# Parse the fixed-width ARIS .txt input file into structured driver data and export to Excel-compatible XML
def parse_aris_txt_to_xml(input_txt, output_xml):
    data = []
    current_driver = {}
    collecting_comments = []

    # Read all lines from the input file
    with open(input_txt, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    for line in lines:
        record_type = line[34:40]

        # Main driver info block
        if record_type == "100001":
            if current_driver:
                # Append AIR BRAKE ENDORSEMENT as class Z and finalize comment block
                if "AIR BRAKE ENDORSEMENT" in collecting_comments:
                    if not current_driver["Class"].endswith("Z"):
                        current_driver["Class"] += "Z"
                    collecting_comments.remove("AIR BRAKE ENDORSEMENT")
                current_driver["Comments"] = "; ".join(collecting_comments)
                data.append(current_driver)
                collecting_comments = []

            # Extract and format driver details
            raw_Licence = line[47:62].strip()
            formatted_Licence = f"{raw_Licence[:5]}-{raw_Licence[5:10]}-{raw_Licence[10:]}"
            current_driver = {
                "Client Name": line[68:98].strip(),
                "Driver Licence Number": formatted_Licence,
                "Class": line[108:112].strip().replace("*", ""),
                "Expiry Date": f"20{line[193:195]}-{line[195:197]}-{line[197:199]}",
                "Licence Status": line[115:193].strip(),
                "Medical Due Date": "",
                "Comments": ""
            }

        # Additional record lines — medical due and comments
        elif record_type == "210001":
            if "MEDICAL DUE DATE" in line:
                raw = line[68:74].strip()
                if raw.isdigit() and len(raw) == 6:
                    current_driver["Medical Due Date"] = f"20{raw[0:2]}-{raw[2:4]}-{raw[4:6]}"
            if line[68:75] == "9999991":
                comment = line[75:128].strip()
                if comment and "ACTIONS COUNT" not in comment:
                    collecting_comments.append(comment)

    # Final driver record (last entry in file)
    if current_driver:
        if "AIR BRAKE ENDORSEMENT" in collecting_comments:
            if not current_driver["Class"].endswith("Z"):
                current_driver["Class"] += "Z"
            collecting_comments.remove("AIR BRAKE ENDORSEMENT")
        current_driver["Comments"] = "; ".join(collecting_comments)
        data.append(current_driver)

    # Convert to DataFrame and export as Excel 2003-compatible XML
    df = pd.DataFrame(data)

    Workbook = ET.Element("Workbook", {
        "xmlns": "urn:schemas-microsoft-com:office:spreadsheet",
        "xmlns:o": "urn:schemas-microsoft-com:office:office",
        "xmlns:x": "urn:schemas-microsoft-com:office:excel",
        "xmlns:ss": "urn:schemas-microsoft-com:office:spreadsheet",
        "xmlns:html": "http://www.w3.org/TR/REC-html40"
    })
    Table = ET.SubElement(ET.SubElement(Workbook, "Worksheet", {"ss:Name": "Drivers"}), "Table")

    # XML header row
    header = ET.SubElement(Table, "Row")
    for col in df.columns:
        cell = ET.SubElement(header, "Cell")
        data_elem = ET.SubElement(cell, "Data", {"ss:Type": "String"})
        data_elem.text = col

    # XML data rows
    for _, row in df.iterrows():
        row_elem = ET.SubElement(Table, "Row")
        for val in row:
            cell = ET.SubElement(row_elem, "Cell")
            data_elem = ET.SubElement(cell, "Data", {"ss:Type": "String"})
            data_elem.text = str(val)

    tree = ET.ElementTree(Workbook)
    tree.write(output_xml, encoding="utf-8", xml_declaration=True)
    return df

# Parse an Excel 2003-format XML file and convert it back into a pandas DataFrame
def extract_df_from_xml(file_path):
    namespaces = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}
    tree = ET.parse(file_path)
    root = tree.getroot()
    rows = root.findall('.//ss:Worksheet/ss:Table/ss:Row', namespaces)
    data = []
    for row in rows:
        cells = row.findall('ss:Cell', namespaces)
        row_data = []
        for cell in cells:
            data_element = cell.find('ss:Data', namespaces)
            row_data.append(data_element.text if data_element is not None else '')
        data.append(row_data)

    # Return a DataFrame with predefined columns if file is empty or poorly formatted
    if not data or len(data) < 2:
        return pd.DataFrame(columns=["Client Name", "Driver Licence Number", "Class", "Expiry Date", "Licence Status", "Medical Due Date", "Comments"])
    
    # First row contains headers; rest is data
    return pd.DataFrame(data[1:], columns=data[0])

# Utility to clean and normalize comment fields for accurate comparison
def normalize_comments(text):
    if not isinstance(text, str):
        return []
    items = [i.strip().lower() for i in text.split(';') if i.strip()]
    return sorted(items)

def wipe_folder(folder):
    """Delete all files and subfolders inside `folder`."""
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        try:
            if os.path.isfile(path) or os.path.islink(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
        except Exception as e:
            log(f"⚠️ Unable to delete {path}: {e}")

def cleanup_output_folder(max_age_hours=48):
    """Delete .xml/.xlsx in output older than `max_age_hours`."""
    cutoff = datetime.now() - timedelta(hours=max_age_hours)
    for name in os.listdir(FOLDERS["output"]):
        path = os.path.join(FOLDERS["output"], name)
        if not os.path.isfile(path):
            continue
        if not (name.lower().endswith(".xml") or name.lower().endswith(".xlsx")):
            continue
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff:
                os.remove(path)
                log(f"🧹 Deleted old output file (>48h): {path}")
        except Exception as e:
            log(f"⚠️ Could not delete {path}: {e}")

# Dataloader Excel generator
def generate_assetworks_xml(df_today):
    """
    Generate AssetWorks-compatible Excel 2003 .xml file from df_today.
    Output path: FOLDERS["data_loader"]/ARIS_upload_YYYY-MM-DD.xml
    """

    # Load employee asset list to retrieve Operator IDs
    asset_file = os.path.join(FOLDERS["assets"], "Active Operator List.csv")
    if not os.path.exists(asset_file):
        print(f"❌ Employee asset file not found: {asset_file}")
        return

    df_assets = pd.read_csv(
        asset_file,
        dtype={"LicenceNo": str, "OperatorID": str}
    )

    # Normalize licence numbers for matching
    def normalize_licence_number(lic):
        return str(lic).replace("-", "").replace(" ", "")

    df_today["LicenceKey"]  = df_today["Driver Licence Number"].apply(normalize_licence_number)
    df_assets["LicenceKey"] = df_assets["LicenceNo"].apply(normalize_licence_number)

    # Merge today's data with asset list on normalized licence number
    df_merged = pd.merge(df_today, df_assets, on="LicenceKey", how="left")

    # Ensure OperatorID is a clean integer-like string (no trailing '.0')
    df_merged["OperatorID"] = (
        df_merged["OperatorID"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
    )

    # Set output path
    today_str = datetime.today().strftime("%Y-%m-%d")
    output_dir = FOLDERS["data_loader"]
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"ARIS_upload_{today_str}.xml")

    _bad = df_merged["OperatorID"].fillna("").astype(str).str.contains(r"\.")
    if _bad.any():
        raise ValueError(f"OperatorID still contains decimals for {_bad.sum()} row(s) - Not able to upload.")

    # Build Excel 2003-compatible XML
    Workbook = ET.Element("Workbook", {
        "xmlns": "urn:schemas-microsoft-com:office:spreadsheet",
        "xmlns:o": "urn:schemas-microsoft-com:office:office",
        "xmlns:x": "urn:schemas-microsoft-com:office:excel",
        "xmlns:ss": "urn:schemas-microsoft-com:office:spreadsheet",
        "xmlns:html": "http://www.w3.org/TR/REC-html40"
    })

    Worksheet = ET.SubElement(Workbook, "Worksheet", {"ss:Name": "Sheet1"})
    Table = ET.SubElement(Worksheet, "Table")

    # Header row
    headers = ["2022", "101:2", "104:10", "104:6", "104:8", "104:15", "104:20"]
    Row = ET.SubElement(Table, "Row")
    for header in headers:
        Cell = ET.SubElement(Row, "Cell")
        Data = ET.SubElement(Cell, "Data", {"ss:Type": "String"})
        Data.text = header

    # Data rows
    for _, row in df_merged.iterrows():
        Row = ET.SubElement(Table, "Row")
        values = [
            "[u:1]",
            row.get("OperatorID", "UNKNOWN"),
            today_str,
            row["Expiry Date"],
            row["Class"],
            row["Medical Due Date"],
            row["Comments"].strip() if pd.notna(row["Comments"]) and row["Comments"].strip() else "NONE"
        ]
        for val in values:
            Cell = ET.SubElement(Row, "Cell")
            Data = ET.SubElement(Cell, "Data", {"ss:Type": "String"})
            Data.text = str(val) if pd.notna(val) else ""

    # Output formatted XML
    rough_string = ET.tostring(Workbook, encoding='utf-8')
    reparsed = minidom.parseString(rough_string)
    pretty_xml_str = reparsed.toprettyxml(indent="  ")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(pretty_xml_str)

    print(f"✅ AssetWorks .xml upload file generated: {file_path}")
    
# Error Detector
# Compares today's and yesterday's driver data to detect changes or issues in licence, medical, and status fields
def compare_dfs(df1, df2):

    # Normalize and index by licence number for reliable comparison
    df1["Driver Licence Number"] = df1["Driver Licence Number"].apply(normalize_Licence_number)
    df2["Driver Licence Number"] = df2["Driver Licence Number"].apply(normalize_Licence_number)
    df1 = df1.set_index("Driver Licence Number")
    df2 = df2.set_index("Driver Licence Number")

    # Store changes by category
    changes = {
    "class": [], "status": [], "comments": [],
    "expiring_licences": [], "expiring_medicals": [], "errors": []
    }

    today_ids = set(df1.index)
    for driver_id in today_ids:
        if driver_id not in df2.index:
            changes["errors"].append(f"Driver not found in yesterday’s data: {driver_id}")
            continue

        row1 = df1.loc[driver_id]
        row2 = df2.loc[driver_id]

        # Compare class, status, and comments
        if row1["Class"] != row2["Class"]:
            changes["class"].append(driver_id)
        if row1["Licence Status"] != row2["Licence Status"]:
            changes["status"].append(driver_id)
        if normalize_comments(row1["Comments"]) != normalize_comments(row2["Comments"]):
            changes["comments"].append(driver_id)

        # Check for upcoming or expired driver licence
        try:
            expiry = datetime.strptime(row1["Expiry Date"], "%Y-%m-%d").date()
            if (expiry - today).days <= EXPIRY_WINDOW_DAYS:
                changes["expiring_licences"].append(driver_id)
        except:
            changes["errors"].append(f"Invalid expiry date for {driver_id}")

        # Check for upcoming or expired medical due
        try:
            med_due = datetime.strptime(row1["Medical Due Date"], "%Y-%m-%d").date()
            if (med_due - today).days <= EXPIRY_WINDOW_DAYS:
                changes["expiring_medicals"].append(driver_id)
        except:
            pass

    # Count how many operators are not currently licensed
    unlicensed_count = len(df1[df1["Licence Status"].str.upper() != "LICENCED"])
    return changes, len(df1), unlicensed_count

# === MAIN SCRIPT ===
# Start the logging process and validate input files
start_time = datetime.now()
log(f"=== DAILY DRIVER CHECK START ===")
log(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

# Check for today's input file and exit if not found
if not os.path.exists(input_file):
    log(f"❌ Input file not found: {input_file}")
    sys.exit(1)

# Parse ARIS .txt input into XML and DataFrame
df_today = parse_aris_txt_to_xml(input_file, today_xml)

# Load yesterday’s parsed file if it exists, else use an empty frame
if not os.path.exists(yesterday_xml):
    log(f"⚠️ Yesterday’s file not found: {yesterday_xml}")
    df_yesterday = pd.DataFrame(columns=df_today.columns)
else:
    df_yesterday = extract_df_from_xml(yesterday_xml)

# Run the comparison logic to extract all changes and summaries
changes, total_today, total_unlicenced = compare_dfs(df_today, df_yesterday)
contains_suspended = False
for driver_id in changes["status"]:
    if driver_id in df_today["Driver Licence Number"].apply(normalize_Licence_number).values:
        status_val = df_today[df_today["Driver Licence Number"].apply(normalize_Licence_number) == driver_id]["Licence Status"].values[0]
        if "SUSPENDED" in status_val.upper():
            contains_suspended = True
            break
log(f"Total operators parsed: {total_today}")
log(f"Total unlicenced operators: {total_unlicenced}")

# Load the employee reference list (with operator IDs)
df_employees = load_employee_csv()

# Generate AssetWorks-compatible upload XML
generate_assetworks_xml(df_today)

# === PURGE OLD DataLoad_21.1.x ARTIFACTS (KEEP ONLY TODAY) ===
today_str = datetime.today().strftime("%Y-%m-%d")
for name in os.listdir(FOLDERS["data_loader"]):
    # Only target DataLoader artifacts
    if not (name.startswith("ARIS_upload_") and (name.endswith(".xml") or name.endswith("-processed.txt"))):
        continue
    # Keep today's files, delete everything else
    if f"ARIS_upload_{today_str}" not in name:
        path = os.path.join(FOLDERS["data_loader"], name)
        try:
            os.remove(path)
            log(f"🗑️ Deleted old DataLoader file: {path}")
        except Exception as e:
            log(f"⚠️ Could not delete {path}: {e}")

# Generate today's filename
today_str = datetime.today().strftime("%Y-%m-%d")
xml_filename = f"ARIS_upload_{today_str}.xml"

# Define server and paths
server_address = f"{SERVER_HOST}:{SERVER_PORT}"
target_path = os.path.join(FOLDERS["data_loader"], xml_filename)
bat_file_path = os.path.join(FOLDERS["data_loader"], "runfile.bat")
logs_path = os.path.join(FOLDERS["data_loader"], "logs")
os.makedirs(logs_path, exist_ok=True)

# Check if server is online
server_online = is_server_online(SERVER_HOST, SERVER_PORT)
upload_success = False
uploaded_count = 0
upload_failures = []
fa_exit_code = None

if server_online:
    log(f"✅ Server {server_address} is reachable. Proceeding with upload.")

    # Step 1: Write batch file (UNC-safe via pushd)
    try:
        data_loader_dir = FOLDERS["data_loader"]  
        bat_content = f"""@echo off
    setlocal
    pushd "{data_loader_dir}"
    REM Now we're in a temp drive letter that maps the UNC; relative paths work.

    START "" /WAIT FADATALOADER.EXE -n "10" -l "logs" -a "{server_address}" -u "{FA_USER}" -p "{FA_PASS}" -i "{xml_filename}"

    popd
    endlocal
    """
        with open(bat_file_path, "w", encoding="utf-8", newline="\r\n") as f:
            f.write(bat_content)

        log(f"✅ Updated runfile.bat for: {xml_filename}")
    except Exception as e:
        log(f"❌ Failed to write runfile.bat: {e}")
        upload_failures.append(f"❌ BAT creation failed: {e}")

    # Step 2: Launch Data Loader (do NOT set cwd to a UNC)
    try:
        flags = getattr(subprocess, "CREATE_NEW_CONSOLE", 0)
        result = subprocess.run(
            ["cmd.exe", "/c", bat_file_path],
            creationflags=flags,
            timeout=300
        )
        fa_exit_code = result.returncode
        log(f"✅ runfile.bat executed (FA exit code: {fa_exit_code})")
        if fa_exit_code != 0:
            upload_failures.append(f"⚠️ DataLoader exit code: {fa_exit_code}")
    except Exception as e:
        log(f"❌ Failed to execute runfile.bat: {e}")
        upload_failures.append(f"❌ BAT launch failed: {e}")

    # Step 3: Check for processed confirmation (poll up to ~90s)
    processed_file = os.path.join(FOLDERS["data_loader"], f"ARIS_upload_{today_str}-processed.txt")

    found_processed = False
    for _ in range(30):  # ~90 seconds max
        if os.path.exists(processed_file):
            found_processed = True
            break
        time.sleep(3)

    upload_success = (fa_exit_code == 0 and found_processed)

    if upload_success:
        uploaded_count = total_today
        log(f"✅ Upload confirmed: {processed_file} found and FA exit code 0")
    else:
        if not found_processed:
            log(f"⚠️ Upload may have failed: {processed_file} not found")
            upload_failures.append("⚠️ No confirmation file generated")
        if fa_exit_code is None:
            upload_failures.append("⚠️ FA did not run (no exit code)")
        elif fa_exit_code != 0:
            upload_failures.append(f"⚠️ FA exit code: {fa_exit_code}")

    # Also require a fresh, non-empty FA log (written in the last 10 minutes)
    fa_logs_root = os.path.join(FOLDERS["data_loader"], "logs")
    latest_fa_log, latest_mtime = None, 0.0
    if os.path.exists(fa_logs_root):
        for root, _, files in os.walk(fa_logs_root):
            for name in files:
                if name.lower().endswith(".txt"):
                    p = os.path.join(root, name)
                    try:
                        m = os.path.getmtime(p)
                        if m > latest_mtime:
                            latest_mtime, latest_fa_log = m, p
                    except Exception:
                        pass

    fa_log_ok = False
    if latest_fa_log and os.path.exists(latest_fa_log):
        try:
            size_ok = os.path.getsize(latest_fa_log) > 0
            recent_ok = (time.time() - latest_mtime) <= 600  # 10 minutes
            fa_log_ok = size_ok and recent_ok
        except Exception:
            fa_log_ok = False

    if upload_success and not fa_log_ok:
        upload_success = False
        upload_failures.append("⚠️ FA log missing, empty, or stale")

else:
    log(f"❌ Server {server_address} is unreachable. Upload step skipped.")
    upload_failures.append(f"❌ Server unreachable: {server_address}")

# === WRITE LOG ===
# Generate the main summary HTML report with consistent styling, summary statistics, and change tables
with open(report_file, "w", encoding="utf-8") as f:

    # Report header and styling
    title_prefix = "**DRIVER SUSPENDED** " if contains_suspended else ""
    f.write(f"<h2>{title_prefix}Driver Licence Change Report – {today}</h2>\n")
    f.write("""
    <style>
        body {
            font-family: Arial, sans-serif;
            font-size: 14px;
            margin: 30px;
        }

        table {
            border-collapse: collapse;
            width: 600px;
            table-layout: fixed;
            margin-bottom: 0;
        }

        table + table {
            margin-top: 20px; /* Add space between tables */
        }

        th {
            width: 200px; /* Fixed label column width */
            background-color: #f2f2f2;
            border: 1px solid #999;
            padding: 10px;
            font-size: 14px;
            text-align: left;
            vertical-align: top;
        }

        td {
            width: 400px; /* Fixed data column width */
            border: 1px solid #999;
            padding: 10px;
            font-size: 14px;
            text-align: left;
            vertical-align: top;
            word-wrap: break-word;
        }

        h3 {
            margin-top: 50px;
            margin-bottom: 10px;
            font-size: 18px;
        }

        ul {
            margin-bottom: 30px;
        }

    </style>
    """)

    # Start time and summary list of detected changes
    f.write(f"<p><b>Start:</b> {start_time}</p>\n")

    # Only show server status if it is DOWN
    if not server_online:
        f.write(
            f"<p style='color:darkred;'><b>Server Status:</b> ❌ "
            f"{SERVER_HOST}:{SERVER_PORT} is UNREACHABLE — upload skipped</p>\n"
        )

    f.write("<ul>")
    f.write(f"<li>Total operators pulled from parser: {total_today}</li>")
    f.write(f"<li>Total operators unlicenced: {total_unlicenced}</li>")
    f.write(f"<li>Total operators with class changes: {len(changes['class'])}</li>")
    f.write(f"<li>Total operators with endorsement/restriction changes: {len(changes['comments'])}</li>")
    f.write(f"<li>Total operators with licence status changes: {len(changes['status'])}</li>")
    f.write(f"<li>Total operators within {EXPIRY_WINDOW_DAYS} days of valid expiry: {len(changes['expiring_licences'])}</li>")
    f.write(f"<li>Total operators within {EXPIRY_WINDOW_DAYS} days of medical expiry: {len(changes['expiring_medicals'])}</li>")
    f.write("</ul>")

    # === Unlicenced Operators (details) ===
    if total_unlicenced > 0:
        f.write("<h3>Unlicenced Operators</h3>")

        # Pull all rows from today's data where status != LICENCED
        unlic_df = df_today.copy()
        # df_today at this point has licence numbers normalized; pull raw formatted number from original cols
        # Re-read formatted licence numbers for display
        unlic_df["LicenceRaw"] = unlic_df.index if "Driver Licence Number" not in unlic_df.columns else unlic_df["Driver Licence Number"]
        if "Driver Licence Number" in unlic_df.columns:
            pass  # already present
        else:
            # If we lost the column during normalization, recover from index
            unlic_df["Driver Licence Number"] = unlic_df.index

        unlic_df = unlic_df[unlic_df["Licence Status"].str.upper() != "LICENCED"]

        # Build a lookup on the employee master by normalised licence
        emp_lookup = df_employees.copy()
        emp_lookup["LicenceKey"] = emp_lookup["LicenceNo"].apply(normalize_Licence_number)

        for _, row in unlic_df.iterrows():
            lic_norm = normalize_Licence_number(row["Driver Licence Number"])
            disp_lic = f"{lic_norm[:5]}-{lic_norm[5:10]}-{lic_norm[10:]}" if len(lic_norm) == 15 else row["Driver Licence Number"]

            hit = emp_lookup[emp_lookup["LicenceKey"] == lic_norm]
            if not hit.empty:
                emp = hit.iloc[0]
                name = emp["OperatorName"]
                op_id = emp["OperatorID"]
                dept_id = emp["DepartmentID"]
                dept_name = emp.get("DepartmentName", "UNKNOWN")
            else:
                # Fallback if not in employee master
                name = row.get("Client Name", "UNKNOWN")
                op_id = "UNKNOWN"
                dept_id = "UNKNOWN"
                dept_name = "UNKNOWN"

            comments = (row.get("Comments") or "").strip() or "NONE"
            status = (row.get("Licence Status") or "").strip() or "UNKNOWN"

            # Render like "Operators With Changes": a 2-column stacked table
            f.write(f"""
            <table>
                <tr><th>Employee</th><td>{escape(str(name))} (ID: {escape(str(op_id))})</td></tr>
                <tr><th>Department</th><td>{escape(str(dept_name))} (ID: {escape(str(dept_id))})</td></tr>
                <tr><th>Licence Status</th><td>{escape(status)}</td></tr>
                <tr><th>Driver Licence Number</th><td>{escape(disp_lic)}</td></tr>
                <tr><th>Comments</th><td>{escape(comments)}</td></tr>
            </table>
            """)

    f.write("<h3>Operators With Changes</h3>")
    changes_written = 0

    # Normalize and re-index for fast lookups
    df_today["Driver Licence Number"] = df_today["Driver Licence Number"].apply(normalize_Licence_number)
    df_today_indexed = df_today.set_index("Driver Licence Number")
    df_yesterday["Driver Licence Number"] = df_yesterday["Driver Licence Number"].apply(normalize_Licence_number)
    df_yesterday_indexed = df_yesterday.set_index("Driver Licence Number")
    
    # Loop through all change categories and generate formatted tables for each affected operator
    for category, driver_ids in changes.items():
        if category == 'errors':
            continue
        for driver_id in driver_ids:
            match = df_employees[df_employees['LicenceNo'].apply(normalize_Licence_number) == driver_id]
            if not match.empty:
                emp = match.iloc[0]
                col_name = {
                    "class": "Class",
                    "status": "Licence Status",
                    "comments": "Comments",
                    "expiring_licences": "Expiry Date",
                    "expiring_medicals": "Medical Due Date"
                }.get(category)
                if driver_id in df_yesterday_indexed.index and driver_id in df_today_indexed.index:
                    old_val = df_yesterday_indexed.loc[driver_id][col_name]
                    new_val = df_today_indexed.loc[driver_id][col_name]

                    # Special logic for expiry-related changes
                    if category in ["expiring_licences", "expiring_medicals"] and old_val == new_val:
                        try:
                            expiry_date = datetime.strptime(new_val, "%Y-%m-%d").date()
                            days_left = (expiry_date - today).days
                            if days_left < 0:
                                change_text = f"EXPIRED {abs(days_left)} DAYS AGO (Expiry Date: {new_val})"
                            elif days_left == 0:
                                change_text = f"EXPIRES TODAY (Expiry Date: {new_val})"
                            else:
                                change_text = f"APPROACHING IN {days_left} DAYS (Expiry Date: {new_val})"
                        except:
                            change_text = new_val
                    else:
                        change_text = f"{old_val} → {new_val}"

                    # Format licence number and comments
                    lic = df_today[df_today['Driver Licence Number'] == driver_id]['Driver Licence Number'].values[0]
                    lic_formatted = f"{lic[:5]}-{lic[5:10]}-{lic[10:]}"
                    comments = df_today[df_today['Driver Licence Number'] == driver_id]['Comments'].values[0]
                    comments = comments if comments.strip() else "NONE"

                    # Output formatted table block
                    f.write(f"""
                    <table>
                        <tr><th>Employee</th><td>{emp['OperatorName']} (ID: {emp['OperatorID']})</td></tr>
                        <tr><th>Department</th><td>{emp['DepartmentName']} (ID: {emp['DepartmentID']})</td></tr>
                        <tr><th>Change Type</th><td>{category.replace('_', ' ').upper()}</td></tr>
                        <tr><th>Old → New</th><td>{change_text}</td></tr>
                        <tr><th>Driver Licence Number</th><td>{lic_formatted}</td></tr>
                        <tr><th>Comments</th><td>{comments}</td></tr>
                    </table>
                    """)
                    changes_written += 1

    if changes_written == 0:
        f.write("<p>NONE</p>")

    # Handle and display errors detected during comparison
    if changes["errors"]:
        f.write("<h3 style='color: darkred;'>Errors</h3><ul>")
        for e in changes["errors"]:
            if "Driver not found in yesterday’s data" in e:
                raw_id = e.split(": ")[1]
                formatted_id = f"{raw_id[:5]}-{raw_id[5:10]}-{raw_id[10:]}"
                match = df_today[df_today["Driver Licence Number"].str.replace("-", "") == raw_id]
                if not match.empty:
                    op = match.iloc[0]
                    f.write(f"<li>Driver not found in yesterday’s data: {formatted_id} – {op['Client Name']}</li>")
                else:
                    f.write(f"<li>Driver not found in yesterday’s data: {formatted_id}</li>")
            else:
                f.write(f"<li>{e}</li>")
        f.write("</ul>")

    # Optionally delete yesterday’s XML file if configured
    if os.path.exists(yesterday_xml) and DELETE_YESTERDAY_OUTPUT:
        os.remove(yesterday_xml)

     # Add explicit AssetWorks upload result at the bottom
    upload_line = "AssetWorks upload: DONE" if upload_success else "❌ AssetWorks upload: NOT CONFIRMED"
    if upload_failures:
        upload_line += " — " + " | ".join(upload_failures)
    f.write(f"<p style='margin-top:30px; margin-bottom:0;'><b>{upload_line}</b></p>")

    # === Append FA DataLoader "Summary" log into the email ===
    try:
        # Prefer today's Summary in ...\logs\2022 (FA import code "2022")
        dl_logs_2022 = os.path.join(FOLDERS["data_loader"], "logs", "2022")
        fa_log_for_email = None
        pattern_prefix = f"ARIS_upload_{today_str}-2022-"
        if os.path.isdir(dl_logs_2022):
            candidates = [
                name for name in os.listdir(dl_logs_2022)
                if name.endswith("-Summary.txt") and name.startswith(pattern_prefix)
            ]
            if candidates:
                candidates.sort(key=lambda n: os.path.getmtime(os.path.join(dl_logs_2022, n)), reverse=True)
                fa_log_for_email = os.path.join(dl_logs_2022, candidates[0])

        # Fallback: latest .txt anywhere under ...\logs (any year/subfolder)
        if not fa_log_for_email:
            fa_logs_root = os.path.join(FOLDERS["data_loader"], "logs")
            newest_path, newest_m = None, 0.0
            if os.path.exists(fa_logs_root):
                for root, _, files in os.walk(fa_logs_root):
                    for name in files:
                        if not name.lower().endswith(".txt"):
                            continue
                        p = os.path.join(root, name)
                        try:
                            m = os.path.getmtime(p)
                            if m > newest_m:
                                newest_m, newest_path = m, p
                        except Exception:
                            pass
            fa_log_for_email = newest_path

        f.write("<p style='margin:0;'><b>AssetWorks Loader Summary Log</b></p>")
        if fa_log_for_email and os.path.exists(fa_log_for_email):
            with open(fa_log_for_email, "r", encoding="utf-8", errors="ignore") as lfp:
                content = lfp.read()
            max_lines = 400
            lines = content.splitlines()
            if len(lines) > max_lines:
                content = "\n".join(["(…truncated… last 400 lines)"] + lines[-max_lines:])

            # Escape for HTML and show nicely
            f.write(
                "<div style='border:1px solid #ccc; background:#fafafa; padding:10px; margin-top:6px;'>"
                f"<pre style='white-space:pre-wrap; margin:0; font-size:14px;'>{escape(content)}</pre>"
                "</div>"
            )
        else:
            f.write("<p><i>No FA DataLoader log was found for today.</i></p>")
    except Exception as e:
        f.write(f"<p style='color:darkred;'><b>⚠️ Failed to include FA DataLoader log:</b> {escape(str(e))}</p>")

    # Mark report end time
    f.write(f"<p><b>End:</b> {datetime.now()}</p>")

# === SEND MAIN SUMMARY EMAIL ===
try:
    with open(report_file, "r", encoding="utf-8") as rf:
        html_body = rf.read()

    # subject line — add server-down flag when offline
    subject = f"Driver Licence Change Report – {today}"
    if not server_online:
        subject += " [SERVER DOWN]"

    send_email_html(EMAIL_RECIPIENTS, subject, html_body)
except Exception as e:
    log(f"❌ Failed to prepare/send email: {e}")

# === GENERATE INDIVIDUAL OPERATOR EMAILS ===
# Creates a separate HTML file for each operator affected by any change.
# These files are saved in a designated folder and can be used as individual email bodies.
for category, driver_ids in changes.items():
    if category == 'errors':
        continue
    for driver_id in driver_ids:
        # Match operator info from master employee CSV
        match = df_employees[df_employees['LicenceNo'].apply(normalize_Licence_number) == driver_id]
        if not match.empty:
            emp = match.iloc[0]

            # Identify which field was changed
            col_name = {
                "class": "Class",
                "status": "Licence Status",
                "comments": "Comments",
                "expiring_licences": "Expiry Date",
                "expiring_medicals": "Medical Due Date"
            }.get(category)

            # Proceed only if both old and new values exist
            if driver_id in df_yesterday_indexed.index and driver_id in df_today_indexed.index:
                old_val = df_yesterday_indexed.loc[driver_id][col_name]
                new_val = df_today_indexed.loc[driver_id][col_name]
                
                # Build safe filename using operator name and change type
                operator_name_safe = re.sub(r'[\\/*?:"<>|]', "_", emp['OperatorName']).replace(",", "").replace(" ", "_")
                filename = os.path.join(FOLDERS["emails"], f"{operator_name_safe}_{category}.html")

                with open(filename, "w", encoding="utf-8") as indf:
                    # Construct appropriate description for expiry-related alerts
                    if category in ["expiring_licences", "expiring_medicals"] and old_val == new_val:
                        try:
                            expiry_date = datetime.strptime(new_val, "%Y-%m-%d").date()
                            days_left = (expiry_date - today).days
                            if days_left < 0:
                                change_text = f"EXPIRED {abs(days_left)} DAYS AGO (Expiry Date: {new_val})"
                            elif days_left == 0:
                                change_text = f"EXPIRES TODAY (Expiry Date: {new_val})"
                            else:
                                change_text = f"APPROACHING IN {days_left} DAYS (Expiry Date: {new_val})"

                        except:
                            change_text = new_val
                    else:
                        change_text = f"{old_val} → {new_val}"

                    # Write styled HTML table for the individual operator
                    indf.write(f"""
                        <style>
                            body {{ 
                                font-family: Arial, sans-serif; 
                                margin: 30px;
                                max-width: 100%;
                                word-wrap: break-word;
                            }}
                            table {{ 
                                border-collapse: collapse; 
                                width: 100%; 
                                max-width: 100%; 
                                table-layout: fixed; 
                                word-break: break-word;
                                margin-bottom: 20px;
                            }}
                            th, td {{ 
                                border: 1px solid #999; 
                                padding: 8px; 
                                font-size: 14px; 
                                text-align: left; 
                                vertical-align: top;
                            }}
                            th {{ 
                                background-color: #f2f2f2; 
                            }}
                            h3 {{ 
                                margin-top: 50px; 
                                margin-bottom: 10px; 
                            }}
                            ul {{ 
                                margin-bottom: 30px; 
                            }}
                        </style>
                        <h3>Driver Licence Change Notification</h3>
                        <p><b>Report Generated:</b> {today}</p>
                        <table>
                            <tr><th>Employee</th><td>{emp['OperatorName']} (ID: {emp['OperatorID']})</td></tr>
                            <tr><th>Department</th><td>{emp['DepartmentName']} (ID: {emp['DepartmentID']})</td></tr>
                            <tr><th>Change Type</th><td>{category.replace('_', ' ').upper()}</td></tr>
                            <tr><th>Old → New</th><td>{change_text}</td></tr>
                        </table>
                    """)

                # === SEND INDIVIDUAL OPERATOR EMAIL ===
                try:
                    with open(filename, "r", encoding="utf-8") as f:
                        html_content = f.read()
                        subject_line = f"[Driver Alert] {emp['OperatorName']} – {category.replace('_', ' ').upper()}"
                        send_email_html(EMAIL_RECIPIENTS, subject_line, html_content)
                except Exception as e:
                    log(f"❌ Failed to send individual email for {emp['OperatorName']}: {e}")

# === WRITE RUN SUMMARY TO DAILY LOG FILE ===
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Locate latest FADataLoader .txt log file (any year folder under logs)
fa_logs_root = os.path.join(FOLDERS["data_loader"], "logs")
latest_fa_log, latest_mtime = None, 0.0
if os.path.exists(fa_logs_root):
    for root, _, files in os.walk(fa_logs_root):
        for name in files:
            if name.lower().endswith(".txt"):
                p = os.path.join(root, name)
                try:
                    m = os.path.getmtime(p)
                    if m > latest_mtime:
                        latest_mtime, latest_fa_log = m, p
                except Exception:
                    pass

# Prepare log content
log_summary = [
    "\n\n" + "=" * 25 + f" RUN: {timestamp} " + "=" * 25,
    f"✅ Total operators parsed: {total_today}",
    f"❗ Total unlicensed operators: {total_unlicenced}",
    "\n➤ Comparison Summary:",
    f"  - Class changes: {len(changes['class'])}",
    f"  - Status changes: {len(changes['status'])}",
    f"  - Endorsement/restriction changes: {len(changes['comments'])}",
    f"  - Expiring licences (within {EXPIRY_WINDOW_DAYS} days): {len(changes['expiring_licences'])}",
    f"  - Expiring medicals (within {EXPIRY_WINDOW_DAYS} days): {len(changes['expiring_medicals'])}",
    f"  - Errors: {len(changes['errors'])}",
    "\n➤ Upload Summary (FADataLoader):",
    f"  - Upload successful: {'Yes' if upload_success else 'No'}"
]

# Add upload failures if any
if upload_failures:
    log_summary.extend(["    • " + failure for failure in upload_failures])

# Append actual FADataLoader log content
log_summary.append("\n➤ FADataLoader Log Output:")
if latest_fa_log and os.path.exists(latest_fa_log):
    try:
        with open(latest_fa_log, "r", encoding="utf-8", errors="ignore") as lf:
            log_summary.append(lf.read())
    except Exception as e:
        log_summary.append(f"⚠️ Failed to read FADataLoader log: {e}")
else:
    log_summary.append("⚠️ No FADataLoader .txt log file found.")

log_summary.append("=" * 60)

# Write to log file (append mode)
with open(log_file, "a", encoding="utf-8") as f:
    f.write("\n".join(log_summary) + "\n")

print(f"✅ Log updated: {log_file}")

# Empty input folder for next drop
try:
    wipe_folder(FOLDERS["input"])
    log("Emptied input folder.")
except Exception as e:
    log(f"⚠️ Failed to empty input folder: {e}")

# Remove output Excel-XML files older than 48h
try:
    cleanup_output_folder(max_age_hours=48)
except Exception as e:
    log(f"⚠️ Output cleanup error: {e}")