# BASIC SCRIPTING WITH BASH AND PYTHON
## INTRODUCTION TO SCRIPTING
### WHAT IS SCRIPTING
Scripting is the process of **writing small programs** —called scripts— that **automate tasks** on a computer. Unlike full-scale software applications, scripts are usually short, focused, and run line by line without needing to be compiled.<br><br>
Key traits of scripts:<br>
- Interpreted, not compiled (they run directly)
- Great for automating repetitive tasks
- Written in languages like Bash, Python, or PowerShell
### WHY AUTOMATE
Automation is about reducing manual effort and human error by letting computers do the work for you.<br><br>
**Reasons to automate:**
- **Consistency:** The same task is done the same way every time
- **Efficiency:** Tasks run faster, even while you sleep
- **Accuracy:** Fewer mistakes compared to manual work
- **Scalability:** Handle large amounts of data or tasks
### DIFFERENCES BETWEEN PROGRAMMING AND SCRIPTING
| **Feature**              | **Programming**                     | **Scripting**                               |
|---------------------------|-------------------------------------|---------------------------------------------|
| **Purpose**               | Build complex apps or systems       | Automate tasks, manage systems              |
| **Language Examples**     | Java, C++, Go                       | Python, Bash, JavaScript (Node), PowerShell |
| **Compile vs Interpret**  | Compiled into binary code           | Interpreted line-by-line at runtime         |
| **Use Case**              | Web apps, mobile apps, games        | File manipulation, automation, batch jobs   |
### WHEN TO USE BASH VS PYTHON
| **Task Type**                  | **Use Bash**                   | **Use Python**                         |
|--------------------------------|--------------------------------|----------------------------------------|
| **System-level tasks**          | Yes                            | Can do, but overkill sometimes         |
| **File and folder manipulation**| Fast and easy                   | More flexibility                       |
| **Working with CSV/JSON data**  | Limited                         | Built-in libraries like pandas, json   |
| **Calling APIs or complex logic**| Difficult                       | Designed for this                      |
| **Scheduling (cron jobs)**      | Perfect fit                     | Works, often called from Bash          |
## BASH SCRIPTING BASICS
Bash is the default command-line shell in many Unix-based systems. You can run individual commands—or group them in a .sh file to create a script.
### STEPS TO CREATE AND RUN A BASH SCRIPT
1.	Create a new file: nano myscript.sh
2.	Add this at the top: #!/bin/bash (shebang line)
3.	Write commands below it
4.	Make it executable: chmod +x myscript.sh
5.	Run it: ./myscript.sh

**Example:**
```bash
#!/bin/bash
echo "Hello, world!"
```
### VARIABLES, CONDITIONALS AND LOOPS
Bash scripts use variables to store and reuse values. They support basic control structures like if, for, and while.<br>
**Variables:**
```bash
#!/bin/bash
name="Alice"
echo "Hello, $name!"
```
**Conditionals:**
```bash
#!/bin/bash
if [ $name == "Alice" ]; then
  echo "Welcome!"
else
  echo "Access denied."
fi
```
**Loops:**
```bash
#!/bin/bash

names=("Jaime" "Ismael" "Esther" "Adrian" "Jorge" "Juan" "Miguel" "Oscar" "Sergio" "Guilherme" "Pedro" "Pedro C" "Tomas")

for name in "${names[@]}"; do
  echo "Name: $name"
done
```
### WORKING WITH FILES AND DIRECTORIES
Bash makes it easy to handle files and folders. Some common commands:<br>
- mkdir data/ → create a directory
- mv file.csv data/ → move a file
- rm old.txt → delete a file
- find . -name "*.log" → find all .log files

**Example:** Copy all .csv files into a backup folder:
```bash
#!/bin/bash
mkdir -p backup
cp *.csv backup/
```
### READING AND WRITING CSVs WITH awk AND cut
While Bash isn’t built for complex data processing, you can do a lot with simple tools like awk, cut, and grep.<br>
**cut —** extract columns from a file:
```bash
#!/bin/bash
cut -d',' -f1,3 files/sales.csv
```
**awk —** pattern scanning and processing:
```bash
#!/bin/bash
awk -F',' '{print $1, $3}' files/sales.csv
```
**grep —** search for lines matching a pattern:
```bash
#!/bin/bash
grep "Shoes" files/sales.csv
```
### SCHEDULING SCRIPTS WITH CRON
Use cron to schedule your scripts to run automatically.<br>
1.	Open the crontab: crontab -e
2.	Add a line like this:<br>
0 6 * * * /home/user/scripts/daily_etl.sh<br>
This runs the script every day at 6:00 AM.
```scss
┬ ┬ ┬ ┬ ┬
│ │ │ │ │
│ │ │ │ └─ Day of week (0–6)
│ │ │ └──── Month (1–12)
│ │ └────── Day (1–31)
│ └──────── Hour (0–23)
└────────── Minute (0–59)
```
How to configure cron schedules: [Crontab Guru](https://crontab.guru/)
### BASH + ETL: AUTOMATING FILE DOWNLOADS OR DAILY JOBS
Bash is great for automating the first part of an ETL process**—Extract**.<br>
**Example script:**
```bash
#!/bin/bash
URL=https://raw.githubusercontent.com/estelaromer/csv-examples/refs/heads/main/data.csv
DATE=$(date +%F)
FILENAME=sales_$DATE.csv

wget $URL -O $FILENAME
mkdir -p data/
mv $FILENAME data/
echo \"Data saved as data/$FILENAME\"
```
Use it with cron to automate daily downloads and move files into folders for processing.
### MINI LAB - BASH: AUTOMATE A CSV DOWNLOAD
**Goal:**<br>
Create a Bash script that:
1.	Downloads a CSV file from a URL
2.	Renames it using today’s date (e.g., sales_2025-07-20.csv)
3.	Moves it into a folder called data/

**Requirements:**
- Internet connection
- Basic Bash shell (Ubuntu, macOS, or Git Bash for Windows)
- The wget or curl command

**Steps:**
1.	Create a new file named download_csv.sh
2.	Add a Bash shebang and comments
3.	Use variables to set the URL and the new filename
4.	Use the date command to generate today’s date
5.	Create the data/ folder if it doesn't exist
6.	Move the downloaded file into that folder

**Example Script: download_csv.sh**
```bash
#!/bin/bash
# Step 1: Set the URL of the CSV file
URL="https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv"

# Step 2: Get today’s date in YYYY-MM-DD format
TODAY=$(date +%F)

# Step 3: Define the new filename with the date
FILENAME="download_$TODAY.csv"

# Step 4: Download the CSV file and rename it
wget -O "$FILENAME" "$URL"

# Step 5: Create the target folder if it doesn't exist
mkdir -p data

# Step 6: Move the file to the folder
mv "$FILENAME" data/

# Step 7: Confirmation message
echo "File downloaded and saved as data/$FILENAME"
```
**Try It Out**
```bash
chmod +x download_csv.sh
./download_csv.sh
```
You should see:<br>
File downloaded and saved as data/download_2025-07-20.csv
### MINI LAB - BASH SCRIPT USING CURL
**Goal:**<br>
Same as before, but now using curl instead of wget to download the file.<br><br>
**Example Script: download_csv_curl.sh**
```bash
#!/bin/bash
# Step 1: Set the URL of the CSV file
URL="https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv"

# Step 2: Get today’s date in YYYY-MM-DD format
TODAY=$(date +%F)

# Step 3: Define the new filename
FILENAME="download_$TODAY.csv"

# Step 4: Use curl to download the file
curl -o "$FILENAME" "$URL"

# Step 5: Check if curl was successful
if [ $? -ne 0 ]; then
  echo "Download failed!"
  exit 1
fi

# Step 6: Create the target folder if it doesn't exist
mkdir -p data

# Step 7: Move the file to the folder
mv "$FILENAME" data/

# Step 8: Confirmation message
echo "File downloaded and saved as data/$FILENAME"
```
**How to run it**
```bash
chmod +x download_csv_curl.sh
./download_csv_curl.sh
```
Expected output:<br>
File downloaded and saved as data/download_2025-07-20.csv<br><br>
**Quick Comparison**<br><br>
wget and curl are the commands that are used to HTTP requests without any GUI or software, rather we use the Terminal in Linux that provides the respective output or message. The commands are very useful for web crawling, web scraping, testing RESTful APIs, etc.<br><br>
**Curl** is a free and open-source command-line utility tool that allows the users as well as the developers to transfer data without any UI interaction. It is commonly used in routers, mobiles, etc.<br><br>
**wget** or GNU wget is another open-source free command-line tool for transferring files using HTTP/HTTPS, FTP, and FTPS.<br><br>
| **Feature**                | **wget**                                         | **curl**                                                    |
|-----------------------------|--------------------------------------------------|-------------------------------------------------------------|
| **Purpose**                 | Primarily used for downloading files            | Used for transferring data to/from servers                  |
| **Protocol Support**        | HTTP, HTTPS, FTP, FTPS, SFTP, and more          | HTTP, HTTPS, FTP, FTPS, SFTP, SCP, LDAP, and more           |
| **Recursive Download**      | Yes, supports recursive downloads               | No                                                          |
| **Authentication**          | Supports basic and digest authentication        | Supports various authentication methods including OAuth      |
| **Resuming Downloads**      | Yes, can resume downloads using `-c`            | Yes, can resume downloads with `-C -`                       |
| **Output Format**           | Saves files directly to disk                    | Outputs data to stdout or saves to a file with `-o`          |
| **Headers**                 | Limited control over headers                    | Extensive control over headers with `-H`                    |
| **Verbosity**               | Simple output, verbose with `-v`                | Detailed output with `-v` and supports tracing               |
| **SSL Certificate Validation** | Validates certificates by default           | Validates certificates by default                            |
| **Cookies**                 | Handles cookies through `--load-cookies`        | Handles cookies easily with `-b` and `-c`                    |
| **Parallel Downloads**      | No                                              | Supports parallel requests with `--parallel` (newer versions) |
| **Conclusion**              | Great for simple, recursive file downloads      | More powerful and flexible for complex HTTP interactions     |
More info [here](https://unix.stackexchange.com/questions/47434/what-is-the-difference-between-curl-and-wget)
## PYTHON SCRIPTING FOR DATA
### VARIABLES, FUNCTIONS, LOOPS
**VARIABLES**<br>
In Python, you don't need to declare the type:
```python
name = "Alice"
price = 19.99
count = 3
```
**FUNCTIONS**<br>
Functions help you organize and reuse code:
```python
def greet(name):
    return f"Hello, {name}!"

print(greet("Bob"))
```
**LOOPS**<br>
For iterating over items:
```python
for i in range(5):
    print(i)

names = ["Ana", "John", "Mia"]
for name in names:
    print(name)
```
### WORKING WITH FILES (open, os, glob)
**[open() —](https://docs.python.org/3/library/functions.html#open)** Read/write local files:
```python
with open("../files/data.txt", "r") as f:
    content = f.read()
    print(content)
```
**[os —](https://docs.python.org/3/library/os.html)** Work with files and folders:
```python
import os
# Create folder
os.mkdir("data")

with open("data/new.txt", "w") as file:
    file.write("Using os module")

# Rename             
os.rename("data/new.txt", "data/changed.txt")
```
**[glob —](https://docs.python.org/3/library/glob.html)** Search for files using wildcards:
```python
from glob import glob

files = glob("files/*.csv")

for file in files:
    print(f"Found file: {file}")
```
### DATA MANIPULATION USING PANDAS
**[pandas](https://pandas.pydata.org/docs/)** is a powerful library for handling data tables (DataFrames). With pandas, you can clean, filter, and analyze datasets with just a few lines.
```python
import pandas as pd
df = pd.read_csv("files/sales.csv")

# Preview data
print(df.head())

# Clean and transform
df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
df["Product"] = df["Product"].str.strip().str.lower()

# Filter rows
df = df[df["Price"] > 0]

# Add new column
df["Total"] = df["Price"] * df["Quantity"]
print(df.head())
```
### HANDLING CSV/JSON FILES
CSV for tables, JSON for structured data like APIs.<br>
**Reading and writing CSVs:**
```python
import pandas as pd

# Read CSV
df = pd.read_csv("files/sales.csv")

# Preview data
print(df.head())

# Clean and transform
df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
df["Product"] = df["Product"].str.strip().str.lower()

# Filter rows
df = df[df["Price"] > 0]

# Add new column
df["Total"] = df["Price"] * df["Quantity"]

print(df.head())

# Write CSV
df.to_csv("files/clean_data.csv", index=False)
```
**Reading and writing JSON:**
```python
import json

# Read JSON
with open("files/data.json") as f:
    data = json.load(f)

print(data)

data["age"] = 43

print(data)

# # Write JSON
with open("files/output.json", "w") as f:
    json.dump(data, f, indent=2)
```

[Link to JSON module](https://docs.python.org/3/library/json.html#module-json)
### APIs WITH REQUESTS
Use the [requests library](https://requests.readthedocs.io/en/latest/) to call REST APIs and handle responses. APIs let your script interact with web services in real time.
```python
import requests

response = requests.get("https://api.github.com/users/octocat")

if response.status_code == 200:
    data = response.json()
    print(data["login"], data["public_repos"])
else:
    print("Failed to fetch data:", response.status_code)
```
### LOGGING AND ERROR HANDLING
**[Logging —](https://docs.python.org/3/library/logging.html#module-logging)** Use logging instead of print() for production-ready scripts. Logs and exceptions help you understand and debug your scripts safely.
```python
import logging
logging.basicConfig(level=logging.INFO)
logging.info("Script started")
logging.warning("Missing value encountered")

# Error Handling with try/except:
try:
    price = float("19.99")
except ValueError:
    logging.error("Failed to convert price")

# Combine both:
try:
    df = pd.read_csv("input.csv")
except FileNotFoundError:
    logging.error("File not found")
    exit(1)
```
### MINI-LAB - PYTHON: CLEAN AND MERGE CSV FILES
**Goal**<br><br>
Create a Python script that:
1.	Reads all .csv files from a folder
2.	Cleans the data using pandas
3.	Merges them into a single cleaned output file

**Prerequisites**
- A folder called data/ with at least 2–3 sample CSV files (e.g., sales_jan.csv, sales_feb.csv, etc.)
- Each file should have the same structure: product, price, quantity

Example content:
```csv
product,price,quantity
Shoes,59.99,3
Hat,NaN,2
backpack,29.90,1
```
**Step-by-Step Script**
```python
# clean_merge_csvs.py
import pandas as pd
import os
from glob import glob

# Step 1: Define the input folder
input_folder = "data"
output_file = "output/merged_cleaned.csv"

# Step 2: Create output folder if it doesn’t exist
os.makedirs("output", exist_ok=True)

# Step 3: Find all CSV files in the folder
csv_files = glob(os.path.join(input_folder, "*.csv"))

# Step 4: Read and clean each file
cleaned_dfs = []

for file in csv_files:
    print(f"Processing {file}")
    df = pd.read_csv(file)
    # Clean data: remove nulls
    df = df.dropna()
    # Normalize column values
    df["product"] = df["product"].str.strip().str.lower()
    # Convert price to float
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    # Re-clean in case price conversion created new nulls
    df = df.dropna()
    cleaned_dfs.append(df)

# Step 5: Merge all cleaned data
merged_df = pd.concat(cleaned_dfs, ignore_index=True)

# Step 6: Save final output
merged_df.to_csv(output_file, index=False)

print(f"Merged file saved as: {output_file}")
```
**How to Run it**
1.	Save the script as clean_merge_csvs.py
2.	Make sure you have the pandas library installed:
```bash
pip install pandas
```
3.	Run the script:
```bash
python3 clean_merge_csvs.py
```
**What This Teaches**
- glob() for reading multiple files
- pandas cleaning tools: dropna, str.strip(), str.lower(), to_numeric
- os.makedirs() to ensure output folders exist
- Data merging with pd.concat()

**Optional Challenges**
- Add logging with logging.info()
- Add command-line arguments using argparse
- Automatically add a column for the file source (e.g., df["source"] = file)
## COMBINING BASH AND PYTHON
Using Bash to control Python scripts is a powerful way to build lightweight automation pipelines. Bash handles system-level orchestration (scheduling, file checking), while Python performs the logic-heavy data processing.
### CALLING PYTHON SCRIPTS FROM BASH
To call a Python script from a Bash script:
```python
#!/bin/bash
echo "Starting Python script..."
python3 process_data.py
echo "Done."
```
This executes the Python file just like a regular command. Make sure the Python script is executable and has a valid shebang (#!/usr/bin/env python3) if you want to call it directly.
### PASSING ARGUMENTS FROM BASH TO PYTHON
You can send values from your Bash script into Python using command-line arguments. This makes your Python scripts reusable for different inputs and scenarios.<br>
[Link to Python sys module](https://docs.python.org/3/library/sys.html)<br>
**Bash Script:**
```bash
#!/bin/bash

FOLDER="data"
OUTPUT="output/cleaned.csv"

python3 bash-python-combined/process_data.py "$FOLDER" "$OUTPUT"
```
**Python Script (process_data.py):**
```python
import sys
import pandas as pd
import os

# Receive arguments
input_folder = sys.argv[1]
output_file = sys.argv[2]

print(f"Reading files from {input_folder}...")
print(f"Saving cleaned data to {output_file}...")
```
### CREATING A FOLDER-WATCHING MINI-PIPELINE
Let’s say you want to check every minute whether new CSV files have been added to a folder. If so, process them.<br>
**Bash Script 1: watch_and_run.sh**
```bash
#!/bin/bash

INPUT_DIR="incoming"
PROCESSED_DIR="processed"
mkdir -p "$PROCESSED_DIR"

# Watch for new files every 60 seconds
while true; do
  for file in "$INPUT_DIR"/*.csv; do
    if [ -f "$file" ]; then
      echo "Processing $file..."
      python3 clean_and_save.py "$file"
      mv "$file" "$PROCESSED_DIR"/
    fi
  done
  sleep 60
done
```
**Python Script: clean_and_save.py**
```python
import sys
import pandas as pd
import os

file_path = sys.argv[1]
print(f"Cleaning file: {file_path}")

df = pd.read_csv(file_path)
df = df.dropna()
df["product"] = df["product"].str.strip().str.lower()

filename = os.path.basename(file_path)
output_path = f"output/cleaned_{filename}"
os.makedirs("output", exist_ok=True)
df.to_csv(output_path, index=False)
print(f"Saved cleaned file to {output_path}")
```

## BEST PRACTICES FOR SCRIPTING
Writing scripts isn’t just about making things work—it’s about making them clear, safe, and maintainable. These practices will help your scripts scale with your team and your data.
### WRITE READABLE SCRIPTS (COMMENTS AND STRUCTURE)
**Why it matters:** Your future self—or your colleagues—should be able to read your code easily. Readable code saves debugging time and builds team trust.<br>
**Best practices:**
- Use clear, descriptive variable names
- Break code into functions or logical blocks
- Add comments explaining why, not just what
- Group related steps together

**Example (Bash):**
```bash
# Download today's sales report and move to the data folder

URL="https://example.com/sales.csv"
curl -o daily_sales.csv "$URL"
mv daily_sales.csv data/
```
**Example (Python):**
```python
# Clean product names and remove invalid prices

df["product"] = df["product"].str.strip().str.lower()
df["price"] = pd.to_numeric(df["price"], errors="coerce")
df.dropna(subset=["price"], inplace=True)
```
### USE set -e AND trap IN BASH
**Why it matters:** Bash scripts should fail safely and visibly when something goes wrong.<br>
**Best practices:**
- set -e → exits the script if any command fails
- trap → define a cleanup or alert when errors happen

**Example:**
```bash
#!/bin/bash
set -e
trap 'echo "Something went wrong. Exiting..."' ERR
echo "Starting ETL job..."
python3 etl_process.py
echo "ETL completed."
```
### USE argparse AND logging IN PYTHON
**Why it matters:** Make your scripts flexible and debuggable.<br>
**[argparse –](https://docs.python.org/3/library/argparse.html#module-argparse)** For command-line arguments:
```python
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
args = parser.parse_args()
print(f"Processing file: {args.input}")
```
**logging –** For consistent output (instead of print()):
```python
import logging

logging.basicConfig(level=logging.INFO)
logging.info("Script started")
```
### USE VERSION CONTROL (GIT BASICS)
**Why it matters:** Track changes, collaborate, and roll back when needed.<br>
**Core [Git](https://git-scm.com/) commands:**
```bash
git init            # Start version control
git add script.py   # Stage changes
git commit -m "Add initial ETL script"
git log             # See history
```
**Extra tips:**
- Use .gitignore to skip folders like ```__pycache__```, *.db, or output/
- Write meaningful commit messages
- Use branches for different versions or experiments
### WHEN TO SCALE TO TOOLS LIKE AIRFLOW
**Scripting is great for:**
- Simple ETL pipelines
- One-time automation
- Learning and prototyping

**But consider orchestration tools when you need:**
- Scheduled, reliable execution (daily/hourly jobs)
- Dependency handling (run this after that)
- Monitoring, retries, notifications
- Complex DAGs (directed acyclic graphs)
## FINAL HANDS-ON PROJECT: AUTOMATED SALES PIPELINE
A guided challenge that combines everything students have learned—Bash, Python, automation, and optional data visualization.
### PROJECT GOAL
Build a mini ETL pipeline that:
1.	Detects new CSV files in a folder
2.	Processes and cleans them using Python (pandas)
3.	Loads the data into a database (SQLite)
4.	(Optional) Visualizes the results in Power BI or Grafana
### DELIVERABLES
Each student/team should deliver:<br><br>
Folder structure:
```bash
automated_sales_pipeline/
├── incoming/
├── archive/
├── db/
├── etl_runner.sh           # Bash script
├── etl_process.py          # Python script
├── sample_data.csv         # Test data
└── README.md               # Instructions
```
### STEP-BY-STEP INSTRUCTIONS
#### BASH SCRIPT: etl_runner.sh
This script:
- Looks for new .csv files in incoming/
- Calls the Python script for each file
- Moves processed files to archive/

**Example:**
```bash
#!/bin/bash

INPUT_DIR="incoming"
ARCHIVE_DIR="archive"
mkdir -p "$INPUT_DIR" "$ARCHIVE_DIR"

for file in "$INPUT_DIR"/*.csv; do
  if [ -f "$file" ]; then
    echo "Processing $file..."
    python3 etl_process.py "$file"
    mv "$file" "$ARCHIVE_DIR"/
    echo "Archived $file"
  fi
done
```
#### PYTHON SCRIPT: etl_process.py
This script:
- Reads the CSV file
- Cleans data using pandas
- Loads it into a SQLite database

**Example:**
```python
import sys
import os
import pandas as pd
import sqlite3

# Input from Bash
file_path = sys.argv[1]

# Read and clean the data
df = pd.read_csv(file_path)
df.dropna(inplace=True)
df["product"] = df["product"].str.strip().str.lower()

# Load into SQLite
os.makedirs("db", exist_ok=True)
conn = sqlite3.connect("db/sales.db")
df.to_sql("sales", conn, if_exists="append", index=False)
conn.close()

print(f"Loaded {len(df)} rows into the database.")
```
#### OPTIONAL VISUALIZATION
**OPTION A - GRAFANA**
- Set up Grafana + SQLite plugin or push to PostgreSQL
- Build a dashboard using SQL queries

**OPTION B - POWER BI**
- Connect to the sales.db SQLite database
- Create charts: total sales, top products, etc.