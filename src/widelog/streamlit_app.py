# Import
from __future__ import annotations
from pathlib import Path
import streamlit as st
import shutil
import os

# Local imports
from widelog.config import load_config
from widelog.imu_csv_export import imu_csv_export
from widelog.source_snapshot import sources_changed
from widelog.refresh_db import refresh_db
from widelog.query_service import (
    snr_exists,
    get_available_stages,
    get_available_test_types,
    get_imu_files,
    get_total_patients,
    get_total_patients_with_imu,
    get_timeline_stages_distribution,
    get_test_types_distribution
)

st.set_page_config(page_title="WideLog IMU CSV Export", layout="centered")

def build_output_path(out_csv: str) -> Path:
    cfg = load_config()
    return Path(cfg.export_dir) / out_csv

# Direct export button function
def export_main_button_csv():
    if st.button("Export Main CSV"):
        cfg = load_config()
        csv_main_path = Path(cfg.csv_main)
        out_path = build_output_path(f"SNR_main.csv")

        try:
            with st.spinner("Exporting..."):
                shutil.copyfile(csv_main_path, out_path)
            
            if out_path.exists():
                st.success(f"Exported successfully: {out_path}")

                st.download_button(
                    label="Download Main CSV",
                    data=out_path.read_bytes(),
                    file_name=out_path.name,
                    mime="text/csv",
                )
            else:
                st.error(f"Export failed. CSV not found at expected path: {out_path}")
            
        except Exception as e:
            st.error(str(e))

# Direct export meta button
def export_meta_button_csv():
    if st.button("Export Meta CSV"):
        cfg = load_config()
        csv_meta_path = Path(cfg.csv_meta)
        out_path = build_output_path(f"SNR_meta.csv")

        try:
            with st.spinner("Exporting..."):
                shutil.copyfile(csv_meta_path, out_path)
            
            if out_path.exists():
                st.success(f"Exported successfully: {out_path}")

                st.download_button(
                    label="Download Meta CSV",
                    data=out_path.read_bytes(),
                    file_name=out_path.name,
                    mime="text/csv",
                )
            else:
                st.error(f"Export failed. CSV not found at expected path: {out_path}")

        except Exception as e:
            st.error(str(e))

def main():
    st.title("WideLog IMU CSV Export (MVP)")

    # Delete old temp files in export directory
    cfg = load_config()
    export_dir = Path(cfg.export_dir)
    if export_dir.exists():
        for file in export_dir.glob("*.csv"):
            os.remove(file)

    ## ----------Dashboard & Stats----------
    col1, col2 = st.columns(2)

    # Graphs
    df_stage = get_timeline_stages_distribution()
    df_type = get_test_types_distribution()

    with col1:
        st.metric("Total Patients in the database", get_total_patients())
        st.subheader("Timeline stage distribution")
        st.dataframe(get_timeline_stages_distribution(), width='stretch')
    
    with col2:
        st.metric("Patients with IMU data", get_total_patients_with_imu())
        st.subheader("Test type distribution")
        st.dataframe(get_test_types_distribution(), width='stretch')

    col3, col4 = st.columns(2)

    with col3:
        export_main_button_csv()

    with col4:
        export_meta_button_csv()

    ### ----------CSV Exporter----------
    # Check if source data has changed since last snapshot
    changed, _, _ = sources_changed()

    if changed:
        st.warning("Source data has changed since last database update. A refresh is recommended.")

        if st.button("Refresh Database"):
            with st.spinner("Refreshing... This may take a few minutes."):
                try:
                    refresh_db()
                    st.success("Database refreshed successfully.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error occurred while refreshing database: {e}")

    else:
        st.success("Database is up to date.")

    # Initialize the output CSV variable
    out_csv = ""

    # Inputs
    snr_id = st.text_input("SNR ID", value="").strip()
    
    # ID check
    if not snr_id:
        st.error("Please enter an SNR ID to load available options.")
        return
    
    # Frontend, check if ID is a number
    if not snr_id.isdigit():
        st.error("The SNR ID must be a number")
        st.stop()

    # Backend, check if id is in the DB
    if not snr_exists(snr_id):
        st.error(f"The SNR {snr_id} not found in the database")
        st.stop()
    

    # DROPDOWN Menu timeline and types
    stages = get_available_stages(snr_id)
    types = get_available_test_types(snr_id)

    timeline_stage = st.selectbox(
        "timeline_stage",
        options = ["all"] + stages
    )

    test_type = st.selectbox(
        "test_type",
        options = ["all"] + types
    )

    if st.button("Export IMU CSV"):
        # For the export function, we want to pass empty strings for "all" to simplify the query logic in imu_csv_export
        stage_arg = "" if timeline_stage == "all" else timeline_stage
        type_arg = "" if test_type == "all" else test_type

        # Display the IMU files matching the criteria
        imu_df = get_imu_files(snr_id, stage_arg, type_arg)

        st.subheader("Matching IMU files")
        st.dataframe(imu_df, width='stretch')

        try:
            with st.spinner("Exporting..."):
                out_csv = imu_csv_export(
                    snr_id=snr_id, 
                    timeline_stage=stage_arg, 
                    test_type=type_arg
                )
            
            out_path = build_output_path(out_csv)
            
            if out_path.exists():
                st.success(f"Exported successfully: {out_path}")

                st.download_button(
                    label="Download IMU CSV",
                    data=out_path.read_bytes(),
                    file_name=out_path.name,
                    mime="text/csv",
                )
            else:
                st.error(f"Export failed. CSV not found at expected path: {out_path}")

        except Exception as e:
            st.error(str(e))


if __name__ == "__main__":
    main()
    