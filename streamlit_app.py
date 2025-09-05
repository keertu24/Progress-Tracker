import streamlit as st
import pandas as pd
import uuid
import datetime
from databricks import sql

# Databricks connection details
DATABRICKS_SERVER = "dbc-7e665065-c769.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/2232b0e6640f4537"
DATABRICKS_TOKEN = "dapifa3a56b6626f4c98e1b989aabc890160"

# Insert record into Databricks
def insert_record(date, topic, hours, status, notes):
    with sql.connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        cursor = connection.cursor()
        cursor.execute(f"""
            INSERT INTO de_learning_progress VALUES (
                '{str(uuid.uuid4())}',
                '{date}',
                '{topic}',
                {hours},
                '{status}',
                '{notes}'
            )
        """)
        cursor.close()

# Fetch records
def fetch_records():
    try:
        with sql.connect(
            server_hostname=DATABRICKS_SERVER,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        ) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM de_learning.tracker_app.de_learning_progress ORDER BY date DESC")
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=cols)
            cursor.close()
        return df
    except Exception as e:
        st.error(f"Databricks error while fetching records: {str(e)}")
        return pd.DataFrame()   # safe fallback


# Streamlit UI
st.title("📊 DE Learning Progress Tracker")

menu = st.sidebar.radio("Menu", ["Add Progress", "View Progress"])

if menu == "Add Progress":
    st.subheader("➕ Add Daily Progress")
    date = st.date_input("Date", datetime.date.today())
    topic = st.text_input("Topic")
    hours = st.number_input("Minutes Spent", min_value=0, step=5)
    status = st.selectbox("Status", ["In Progress", "Completed"])
    notes = st.text_area("Notes")
    
    if st.button("Save Progress"):
        insert_record(date, topic, hours, status, notes)
        st.success("Progress saved successfully!")

elif menu == "View Progress":
    st.subheader("📈 Your Progress")
    df = fetch_records()
    if not df.empty:
        st.dataframe(df)
        st.bar_chart(df.groupby("date")["hours"].sum())
        daily_minutes = df.groupby("date")["hours"].sum().reset_index()
        st.line_chart(daily_minutes, x="date", y="hours")
    else:
        st.info("No progress records found.")
