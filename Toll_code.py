import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL

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
    
    creds = DB_CREDENTIALS[db_name]
    try:
        url = URL.create(
            drivername="postgresql",
            username=creds["username"],
            password=creds["password"],
            host=creds["host"],
            port=creds["port"],
            database=creds["database"]
        )
        engine = create_engine(url)
        return engine
    except Exception as e:
        st.error(f"Failed to connect to {db_name}: {e}")
        return None

def process_file(uploaded_file):
    df_excel = pd.read_excel(uploaded_file)
    
    # Validate required columns
    required_columns = ["Licence Plate No", "Transaction Date Time"]
    if not all(col in df_excel.columns for col in required_columns):
        st.error("Uploaded file must contain 'Licence Plate No' and 'Transaction Date Time' columns.")
        return None
    
    # Standardize column names
    df_excel.rename(columns={"Transaction Date Time": "transaction_datetime", "Licence Plate No": "vehicle_reg_no"}, inplace=True)
    df_excel["transaction_datetime"] = pd.to_datetime(df_excel["transaction_datetime"], errors="coerce")
    df_excel["transaction_date"] = df_excel["transaction_datetime"].dt.date  

    # Connect to trips database
    engine_trips = get_engine("trips")
    if engine_trips:
        try:
            with engine_trips.connect() as conn:
                df_trips = pd.read_sql(text("""
                    SELECT vehicle_reg_no, client_office AS site_name, 
                           actual_start_time AS leave_time, 
                           actual_end_time AS reach_time 
                    FROM etms_trips
                """), conn)

                df_spot_trips = pd.read_sql(text("""
                    SELECT vehicle_reg_no, site_name, 
                           leave_lithium_hub_time AS leave_time, 
                           reach_lithium_hub_time AS reach_time 
                    FROM etms_spot_trips
                """), conn)

            df_trips_combined = pd.concat([df_trips, df_spot_trips], ignore_index=True)
            df_trips_combined[["leave_time", "reach_time"]] = df_trips_combined[["leave_time", "reach_time"]].apply(pd.to_datetime, errors="coerce")

            # Merge and filter transactions within time range
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

    # Connect to Uber database
    engine_uber = get_engine("uber_full")
    if engine_uber:
        try:
            with engine_uber.connect() as conn:
                df_uber = pd.read_sql(text("""
                    SELECT vehicle_number AS vehicle_reg_no, 
                           CAST(trip_request_time AS DATE) AS transaction_date 
                    FROM seven_trip_report
                """), conn)

        except Exception as e:
            st.error(f"Error fetching Uber data: {e}")
            df_uber = pd.DataFrame(columns=["vehicle_reg_no", "transaction_date"])
    else:
        df_uber = pd.DataFrame(columns=["vehicle_reg_no", "transaction_date"])

    # Merge all data
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

    # Save output
    output_path = "output_mapped_updated.xlsx"
    final_output.to_excel(output_path, index=False)
    return output_path

st.title("🚗 Toll Data Mapping App")
st.markdown("Upload an Excel file with **Licence Plate No** and **Transaction Date Time** to map vehicle site names.")

uploaded_file = st.file_uploader("📂 Upload Excel File", type=["xlsx"], help="Ensure the file contains required columns.")

if uploaded_file is not None:
    with st.spinner("Processing file..."):
        output_path = process_file(uploaded_file)
        if output_path:
            st.success("✅ Mapping completed! Download the processed file below.")
            with open(output_path, "rb") as file:
                st.download_button(label="📥 Download Output", data=file, file_name="output_mapped.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
