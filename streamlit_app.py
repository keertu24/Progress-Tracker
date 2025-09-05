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
def insert_record(date, topic, minutes, status, notes):
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
                {minutes},
                '{status}',
                '{notes}'
            )
        """)
        cursor.close()


def update_record(record_id, date, topic, minutes, status, notes):
    with sql.connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        cursor = connection.cursor()
        cursor.execute(f"""
            UPDATE de_learning_progress
            SET date = '{date}',
                topic = '{topic}',
                minutes = {minutes},
                status = '{status}',
                notes = '{notes}'
            WHERE id = '{record_id}'
        """)
        cursor.close()

def delete_record(record_id):
    with sql.connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    ) as connection:
        cursor = connection.cursor()
        cursor.execute(f"""
            DELETE FROM de_learning_progress
            WHERE id = '{record_id}'
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

menu = st.sidebar.radio("Menu", ["Add Progress", "View Progress", "Update Progress", "Delete Progress"])


if menu == "Add Progress":
    st.subheader("➕ Add Daily Progress")
    date = st.date_input("Date", datetime.date.today())
    topic = st.text_input("Topic")
    minutes = st.number_input("Minutes Spent", min_value=0, step=5)
    status = st.selectbox("Status", ["In Progress", "Completed"])
    notes = st.text_area("Notes")
    
    if st.button("Save Progress"):
        insert_record(date, topic, minutes, status, notes)
        st.success("Progress saved successfully!")

elif menu == "View Progress":
    st.subheader("📈 Your Progress")
    df = fetch_records()
    if not df.empty:
        st.dataframe(df)
        st.bar_chart(df.groupby("date")["minutes"].sum())
        daily_minutes = df.groupby("date")["minutes"].sum().reset_index()
        st.line_chart(daily_minutes, x="date", y="minutes")
    else:
        st.info("No progress records found.")
        
elif menu == "Update Progress":
    st.subheader("✏️ Update Progress")
    df = fetch_records()
    if not df.empty:
        record_id = st.selectbox("Select Record ID", df["id"])
        selected = df[df["id"] == record_id].iloc[0]

        date = st.date_input("Date", selected["date"])
        topic = st.text_input("Topic", selected["topic"])
        minutes = st.number_input("Minutes Spent", min_value=0, step=5, value=int(selected["minutes"]))
        status = st.selectbox("Status", ["In Progress", "Completed"], index=0 if selected["status"]=="In Progress" else 1)
        notes = st.text_area("Notes", selected["notes"])

        if st.button("Update"):
            update_record(record_id, date, topic, minutes, status, notes)
            st.success("Record updated successfully!")

elif menu == "Delete Progress":
    st.subheader("🗑 Delete Progress")
    df = fetch_records()
    if not df.empty:
        record_id = st.selectbox("Select Record ID", df["id"])
        if st.button("Delete"):
            delete_record(record_id)
            st.warning("Record deleted successfully!")

