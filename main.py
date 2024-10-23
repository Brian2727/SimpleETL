from datetime import datetime
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
import sqlite3


def log_progress(message, log_file_name):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file_name, "a") as f:
        f.write(timestamp + ',' + message + '\n')


def extract(urls, table_cols):
    df = pd.DataFrame(columns=[table_cols])
    for url in urls:
        response = requests.get(url)
        if response.status_code == 200:
            html = bs(response.text, "html.parser")
            table = html.find_all('table')
            if len(table) >= 3:  # Check if there are at least 3 tables
                table = table[2]
            if table:
                table_body = table.find('tbody')
                table_row = table_body.find_all('tr')
                for row in table_row:
                    # row_content = bs(row,"html.parser")
                    cells = row.find_all('td')
                    name = None
                    MC_USD_Billion = None
                    for index, cell in enumerate(cells):
                        if index == 1:
                            name = cell.text[0:len(cell.text) - 1]
                        elif index == 2:
                            MC_USD_Billion = float(cell.text[0:len(cell.text) - 1].replace(',', ""))
                        elif index > 2:
                            break
                    if name and MC_USD_Billion:
                        new_row = pd.DataFrame({table_cols[0]: [name],
                                                table_cols[1]: [MC_USD_Billion]})
                        df.loc[len(df)] = [name, MC_USD_Billion]
    return df


def transform(df, exange_rate_path):
    ex_rates = pd.read_csv(exange_rate_path)
    for name, rate in zip(ex_rates['Currency'], ex_rates['Rate']):
        df[f"MC_{name}_Billion"] = round(df['MC_USD_Billion'] * float(rate), 2)
        print(df)
    return df


def load_to_csv(df, csv_file):
    df.to_csv(csv_file, index=False)


def load_to_db(df, db):
    conn = sqlite3.connect(db)
    df.to_sql('Largest_banks', conn, if_exists='replace', index_label=True)
    conn.close()


table_cols = ["Name", "MC_USD_Billion"]
final_table_cols = []
urls = ["https://web.archive.org/web/20230908091635", "https://en.wikipedia.org/wiki/List_of_largest_banks"]
exange_rate_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
output_file_path = "./Largest_banks_data.csv"
db = "Banks.db"
log_file_name = "code_log.txt"

log_progress("Extranting Data From URLS", log_file_name)
df = extract(urls, table_cols)
log_progress("Extranting Finished", log_file_name)

log_progress("Transforming Data Adding New Columns", log_file_name)
df = transform(df, exange_rate_path)
log_progress("Finished Transformation", log_file_name)

log_progress(f"Loading To csv named {output_file_path}", log_file_name)
load_to_csv(df, output_file_path)
log_progress("Finished Loading to CSV", log_file_name)

log_progress(f"Loading To Database (SQL) name {db}", log_file_name)
load_to_db(df, db)
log_progress("Finished The loading", log_file_name)

log_progress("Finished", log_file_name)

with sqlite3.connect(db) as c:
    q1 = c.execute("""SELECT * FROM Largest_banks""").fetchall()
    q2 = c.execute("""SELECT AVG("('MC_GBP_Billion',)") FROM Largest_banks""").fetchall()
    q3 = c.execute("""SELECT "('Name',)" from Largest_banks LIMIT 5""").fetchall()

for r in q1:
    print(r)

for r in q2:
    print(r)

for r in q3:
    print(r)
