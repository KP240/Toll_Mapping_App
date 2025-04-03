import streamlit as st
import pandas as pd
import io
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from datetime import datetime



# Get the current year dynamically
current_year = datetime.now().year

# Generate year options dynamically (current year + next 5 years)
year_options = list(range(current_year - 3, current_year + 5))

def get_engine(db_name):
    """Create a SQLAlchemy engine while handling special characters in passwords."""
    DB_CREDENTIALS = {
        "trips": {
            "host": "34.100.223.97",
            "port": "5432",
            "database": "trips",
            "username": "postgres",
            "password": "theimm0rtaL"
        },
        "uber_full": {
            "host": "34.100.216.3",
            "port": "5432",
            "database": "uber_full",
            "username": "kartik",
            "password": "Project@Li3"
        }
    }
    
    creds = DB_CREDENTIALS.get(db_name)
    if not creds:
        st.error(f"Database {db_name} not found in credentials.")
        return None
    
    url = URL.create(
        drivername="postgresql",
        username=creds["username"],
        password=creds["password"],
        host=creds["host"],
        port=creds["port"],
        database=creds["database"]
    )
    return create_engine(url)

def process_file(uploaded_file, selected_year, selected_month):
    df_excel = pd.read_excel(uploaded_file, engine='openpyxl')
    
    required_columns = ["Licence Plate No", "Transaction Date Time"]
    if not all(col in df_excel.columns for col in required_columns):
        st.error("Uploaded file must contain 'Licence Plate No' and 'Transaction Date Time' columns.")
        return None
    
    df_excel.rename(columns={"Transaction Date Time": "transaction_datetime", "Licence Plate No": "vehicle_reg_no"}, inplace=True)
    df_excel["transaction_datetime"] = pd.to_datetime(df_excel["transaction_datetime"], errors="coerce")
    df_excel["transaction_date"] = df_excel["transaction_datetime"].dt.date  
    
    start_date = f"{selected_year}-{selected_month:02d}-01"
    end_date = f"{selected_year}-{selected_month:02d}-31"
    
    engine_trips = get_engine("trips")
    if engine_trips:
        try:
            with engine_trips.connect() as conn:
                df_trips = pd.read_sql(text(f"""
                    SELECT vehicle_reg_no, client_office AS site_name, 
                           actual_start_time AS leave_time, 
                           actual_end_time AS reach_time 
                    FROM etms_trips
                    WHERE trip_date BETWEEN '{start_date}' AND '{end_date}'
                """), conn)

                df_spot_trips = pd.read_sql(text(f"""
                    SELECT vehicle_reg_no, site_name, 
                           leave_lithium_hub_time AS leave_time, 
                           reach_lithium_hub_time AS reach_time 
                    FROM etms_spot_trips
                    WHERE trip_date BETWEEN '{start_date}' AND '{end_date}'
                """), conn)
        
            df_trips_combined = pd.concat([df_trips, df_spot_trips], ignore_index=True)
            df_trips_combined["leave_time"] = pd.to_datetime(df_trips_combined["leave_time"], errors="coerce")
            df_trips_combined["reach_time"] = pd.to_datetime(df_trips_combined["reach_time"], errors="coerce")

            merged_df = df_excel.merge(df_trips_combined, on="vehicle_reg_no", how="left")
            merged_df = merged_df[
                (merged_df["transaction_datetime"] >= merged_df["leave_time"]) & 
                (merged_df["transaction_datetime"] <= merged_df["reach_time"])
            ].drop_duplicates(subset=['vehicle_reg_no', 'transaction_datetime'])
        
        except Exception as e:
            st.error(f"Error fetching trips data: {e}")
            merged_df = pd.DataFrame(columns=["vehicle_reg_no", "transaction_datetime", "site_name"])
    else:
        merged_df = pd.DataFrame(columns=["vehicle_reg_no", "transaction_datetime", "site_name"])
    
    engine_uber = get_engine("uber_full")
    if engine_uber:
        try:
            with engine_uber.connect() as conn:
                df_uber = pd.read_sql(text(f"""
                    SELECT vehicle_number AS vehicle_reg_no, 
                           CAST(trip_request_time AS DATE) AS transaction_date 
                    FROM seven_trip_report
                    WHERE trip_request_time BETWEEN '{start_date}' AND '{end_date}'
                """), conn)
        except Exception as e:
            st.error(f"Error fetching Uber data: {e}")
            df_uber = pd.DataFrame(columns=["vehicle_reg_no", "transaction_date"])
    else:
        df_uber = pd.DataFrame(columns=["vehicle_reg_no", "transaction_date"])
    
    final_output = df_excel.merge(
        merged_df[["vehicle_reg_no", "transaction_datetime", "site_name"]].drop_duplicates(), 
        on=["vehicle_reg_no", "transaction_datetime"], 
        how="left"
    )
    
    if "transaction_date" in df_uber.columns:
        final_output = final_output.merge(
            df_uber.drop_duplicates(), 
            on=["vehicle_reg_no", "transaction_date"], 
            how="left", 
            suffixes=("", "_uber")
        )
    
    final_output["site_name"] = final_output["site_name"].fillna("Not Found")
    final_output = final_output.drop_duplicates()
    
    buffer = io.BytesIO()
    final_output.to_excel(buffer, index=False, engine='openpyxl')
    buffer.seek(0)
    return buffer

st.title("🚗 Toll Data Mapping App")
st.markdown("Upload an Excel file with **Licence Plate No** and **Transaction Date Time** to map vehicle site names.")

selected_year = st.selectbox("Select Year", options=year_options, index=0)
selected_month = st.selectbox("Select Month", options=list(range(1, 13)), format_func=lambda x: datetime(2024, x, 1).strftime('%B'))

uploaded_file = st.file_uploader("📂 Upload Excel File", type=["xlsx"], help="Ensure the file contains required columns.")

if uploaded_file is not None:
    with st.spinner("Processing file..."):
        output_buffer = process_file(uploaded_file, selected_year, selected_month)
        if output_buffer:
            st.success("✅ Mapping completed! Download the processed file below.")
            st.download_button(label="📥 Download Output", data=output_buffer, file_name="output_mapped.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
