import psycopg2
import pandas as pd

conn = psycopg2.connect(host="localhost", dbname="postgres",
                        user="postgres", password="26062001", port=5432)

excel_file = "jan24_tollfreeusage.xlsx"

df_company_name = pd.read_excel(excel_file, sheet_name="Company phone numbers (2)")
df_bulkvs = pd.read_excel(excel_file, sheet_name="Bulkvs")

# Create table for company names
try:
    cur = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS company_phones (
        company VARCHAR(255),
        phone_number VARCHAR(255)
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    print("Table 'company_phones' created or already exists.")
except (Exception, psycopg2.Error) as error:
    print("Error creating table company_phones:", error)
    exit()

# Add data into company_phones database from the Excel sheet
try:
    for index, row in df_company_name.iterrows():
        company = row["Company"]
        phone_number = row["Phone Number"]
        cur.execute("""
                    INSERT INTO company_phones (company, phone_number)
                    VALUES (%s, %s);
                    """,
                    (company, phone_number))
        conn.commit()  # Commit after each insertion
    print("Data inserted successfully into company_phones.")
except (Exception, psycopg2.Error) as error:
    print("Error inserting data into company_phones:", error)
    conn.rollback()  # Rollback changes in case of error

# Create a new table named 'bulk_vs' in PostgreSQL
try:
    create_bulkvs_query = """
    CREATE TABLE IF NOT EXISTS bulk_vs (
        company_name VARCHAR(255),
        duration_secs INTEGER,
        call_destination INTEGER
    );
    """
    cur.execute(create_bulkvs_query)
    conn.commit()
    print("Table 'bulk_vs' created or already exists.")
except (Exception, psycopg2.Error) as error:
    print("Error creating table bulk_vs:", error)
    exit()

# Iterate through the rows of the Bulkvs sheet and insert data into the bulk_vs table
for index, row in df_bulkvs.iterrows():
    phone_number = str(int(row["Call Destination"]))  # Convert to integer first to remove decimal point, then to string
    cur.execute("SELECT company FROM company_phones WHERE phone_number = %s;", (phone_number,))
    result = cur.fetchone()
    if result:
        company_name = result[0]
        duration_secs = row["Duration Secs"]
        call_destination = row["Call Destination"]
        cur.execute("""
                    INSERT INTO bulk_vs (company_name, duration_secs, call_destination)
                    VALUES (%s, %s, %s);
                    """,
                    (company_name, duration_secs, call_destination))
        conn.commit()  # Commit after each insertion

cur.close()
conn.close()
print("Database connection closed.")
