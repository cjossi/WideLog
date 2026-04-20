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
from widelog.reduced_csv_exporter import reduced_csv_exporter
from widelog.refresh_db import refresh_db
from widelog.query_service import (
    snr_exists,
    get_available_stages,
    get_available_test_types,
    get_imu_files,
    get_imu_files_v2,
    get_total_patients,
    get_total_patients_with_imu,
    get_timeline_stages_distribution,
    get_test_types_distribution,
    get_all_characteristics,
    value_exists_objects
)

st.set_page_config(page_title="WideLog IMU CSV Export", layout="centered")

### ---FUNCTIONS--- ###
# This function simply build the output path
def build_output_path(out_csv: str) -> Path:
    cfg = load_config()
    return Path(cfg.export_dir) / out_csv

# This function create a button to export the main CSV file
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

# This function create a button to export the meta CSV file
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

## ---FILTER SELECTION--- ##
# This function create the filters selection for the reduced size CSV export
def filter_objects(filter_nb: int):
    # First selectbox filter creation.
    characteristics = get_all_characteristics()
    filter = None
    filter_values = None

    col1, col2 = st.columns(2)

    with col1: 
        filter = st.selectbox(
            f"Filter {filter_nb}",
            options=characteristics,
            key = f"filter_col_{filter_nb}"         # To keep track of the filter number in the session state
        )

    with col2:
        filter_values = st.text_input(
            f"Filter {filter_nb} values", 
            value="",
            key = f"filter_values_{filter_nb}",      # To keep track of the filter number in the session state
            on_change = update_current_values
        ).strip()

    # Check if the filter values are in the db
    if filter and filter_values:
        if value_exists_objects(filter, filter_values):
            st.success(f"All values for {filter} are valid.")
        else:
            st.error(f"Value '{filter_values}' not found in column '{filter}' of the database.")

    return filter, filter_values

# This function update the current value of the filter in the session state
def update_current_values():
    st.session_state.current_value = st.session_state.text_input_value

# This function add the current filter to the list of filters in the session state
def add_filter_callback():
    current_filter = st.session_state.current_filter
    current_value = st.session_state.current_value

    st.session_state.filters.append((current_filter, current_value))
    st.session_state.filter_nb += 1
    st.session_state.current_filter = None
    st.session_state.current_value = None

# This function remove a filter from the list of filters in the session state
def remove_filter_callback(index):
    st.session_state.filters.pop(index)
    st.session_state.filter_nb = len(st.session_state.filters) + 1

## ---COLUMN SELECTION --- ##
# This function create the column selection for the reduced size CSV export
def export_reduced_column(column_nb: int):
    # First selectbox filter creation.
    characteristics = get_all_characteristics()
    selected_column = None

    selected_column = st.selectbox(
        f"Column {column_nb}",
        options=characteristics,
        key = f"col_{column_nb}"         # To keep track of the column number in the session state
    )

    # No need to check since it's a selectbox, the value will always be valid

    return selected_column

# This function add the current column to the list of columns in the session state
def add_column_callback():
    current_column = st.session_state.current_column
    st.session_state.columns.append(current_column)
    st.session_state.column_nb += 1
    st.session_state.current_column = None

# This function remove a column from the list of columns in the session state
def remove_column_callback(index):
    st.session_state.columns.pop(index)
    st.session_state.column_nb = len(st.session_state.columns) + 1

# This function show a toast message when the user change tab
def change_tab():
    st.toast(f"You opened the {st.session_state.current_tab} tab.")

## ---Functions used in the main function--- ##
# This function display some stats about the database in the dashboard tab
def get_dashboard_stats():
    col1, col2 = st.columns(2)

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

# This function checks if the database has been modified since last snapshot
def modified_database():
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

# This function create a reduced size CSV export with selected columns and filters, and return path to the exported CSV file
def filter_columns_csv_exporter():
    # Add selection of columns to export for the reduced size CSV export
    st.header("Reduced Size CSV Export")
    st.subheader("Filter Selection")
    st.info("Select the filters you want to apply in the reduced size CSV export.")
    ## ---FILERS--- ##
    # Use of session state to keep track of the filters and their number
    if "filters" not in st.session_state:
        st.session_state.filters = []
    if "filter_nb" not in st.session_state:
        st.session_state.filter_nb = 1
    if "text_input_value" not in st.session_state:
        st.session_state.text_input_value = ""
    if "current_value" not in st.session_state:
        st.session_state.current_value = ""

    # Display existing filters
    for i, (filter, filter_value) in enumerate(st.session_state.filters):
        col1, col2 = st.columns([4, 1])

        with col1:
            st.write(f"Filter {i+1}: {filter} = {filter_value}")
        with col2:
            st.button("Remove", key=f"del_filter_{i+1}", on_click = remove_filter_callback, args = (i,))

    # Display the curent filter to add
    st.session_state.current_filter, st.session_state.current_value = filter_objects(filter_nb = st.session_state.filter_nb)

    # Button to ad a filter
    st.button("Add Filter", on_click = add_filter_callback)

    ## ---COLUMNS--- ##
    st.subheader("Column Selection")
    st.info("Select the columns you want to include in the reduced size CSV export.")
    # Use of session state to keep track of the columns and their number
    if "columns" not in st.session_state:
        st.session_state.columns = []
    if "column_nb" not in st.session_state:
        st.session_state.column_nb = 1

    # Display existing column selection
    for i, column in enumerate(st.session_state.columns):
        col1, col2 = st.columns([4, 1])

        with col1:
            st.write(f"Column {i+1}: {column}")
        with col2:
            st.button("Remove", key=f"del_column_{i+1}", on_click = remove_column_callback, args = (i,))

    # Display the current column selection to add
    st.session_state.current_column = export_reduced_column(column_nb= st.session_state.column_nb)

    # Button to add a column to the selection
    st.button("Add Column", on_click = add_column_callback)

    ## ---Button to export the reduced size CSV with the selected columns and filters---
    if st.button("Export Reduced Size CSV"):
        if not st.session_state.columns:
            st.error("Please select at least one column to export.")
        else:

            try:
                with st.spinner("Exporting..."):
                    out_path = reduced_csv_exporter(list_of_columns=st.session_state.columns, list_of_filters=st.session_state.filters)
    
                
                if out_path.exists():
                    st.success(f"Exported successfully: {out_path}")

                    st.download_button(
                        label="Download Reduced Size CSV",
                        data=out_path.read_bytes(),
                        file_name=out_path.name,
                        mime="text/csv",
                    )
                else:
                    st.error(f"Export failed. CSV not found at expected path: {out_path}")

            except Exception as e:
                st.error(str(e))

# This function export the IMU CSV files matching the selected criteria and return the path to the exported CSV file
def csv_imu_exporter():
    st.header("IMU CSV Export")
    st.info("Select the criteria for the IMU CSV export. You can choose to filter by test type and timeline stage, or export all IMU files for a given SNR ID.")

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
    types = get_available_test_types(snr_id)

    test_type = st.selectbox(
        "test_type",
        options = ["all"] + types
    )

    # Initialise timeline stage
    stages_raw = get_available_stages(snr_id)

    # If case of the w3, w6 and w8 that are only for the gait test
    if test_type == "gait":
        stages = stages_raw
        
    elif test_type == "all" or test_type == "dlsm":
        stages = [s for s in stages_raw if s not in ["w3", "w6", "w8"]]
    else:
        st.error("Error: test type not recognized")
        st.stop()

    timeline_stage = st.selectbox(
            "timeline_stage",
            options = ["all"] + stages,
            key = f"timeline_{test_type}"       # To reset the timeline dropdown when the test type is changed
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

# Main function
def main():
    st.title("WideLog IMU CSV Export (MVP)")

    # Delete old temp files in export directory
    cfg = load_config()
    export_dir = Path(cfg.export_dir)
    if export_dir.exists():
        for file in export_dir.glob("*.csv"):
            os.remove(file)

    ###----------TAB-----------------------###
    # Track the current tab in session state to avoid resetting the tab selection after rerun
    if "current_tab" not in st.session_state:
        st.session_state.current_tab = "Dashboard & Stats"
    
    tab1, tab2, tab3 = st.tabs(["Dashboard & Stats", "CSV Reduced Exporter", "CSV IMU Exporter"], on_change = change_tab, default="Dashboard & Stats")

    ###----------Dashboard & Stats---------###
    with tab1:
        # Update the current tab in session state
        st.session_state.current_tab = "Dashboard & Stats"

        get_dashboard_stats()
            

    ###----------CSV Exporter--------------###
    # Check if database has been modified since last snapshot
    modified_database()

    ###----------REDUCED SIZE EXPORTER-----###
    with tab2:
        # Update the curret tab in session state
        st.session_state.current_tab = "CSV Reduced Exporter"

        filter_columns_csv_exporter()


    ###----------CSV IMU Exporter----------###
    with tab3:
        # Update the current tab in session state
        st.session_state.current_tab = "CSV IMU Exporter"

        csv_imu_exporter()



if __name__ == "__main__":
    main()
    