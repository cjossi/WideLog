# Standard library imports
from __future__ import annotations

from io import BytesIO
from pathlib import Path
import shutil
import zipfile

# Third-party imports
import streamlit as st

# Local imports
from widelog.config import load_config
from widelog.constants import (
    ALL,
    GAIT_ONLY_STAGES
)
from widelog.imu_csv_export import imu_csv_export
from widelog.query_service import (
    get_all_characteristics,
    get_available_stages,
    get_available_test_types,
    get_test_types_distribution,
    get_timeline_stages_distribution,
    get_total_patients,
    get_total_patients_with_imu,
    snr_exists,
    value_exists_objects
)
from widelog.reduced_csv_exporter import reduced_csv_exporter
from widelog.refresh_db import refresh_db
from widelog.source_snapshot import sources_changed


st.set_page_config(
    page_title = "WideLog IMU CSV Export",
    layout = "centered"
)


# ============================================================
# Session state helpers
# ============================================================

def initialise_session_state() -> None:
    """
    Initialise Streamli session state variables.
    """

    default = {
        "filters": [],
        "columns": []
    }

    for key, value in default.items():
        if key not in st.session_state:
            st.session_state[key] = value

# ============================================================
# Utility functions
# ============================================================

def build_output_path(filename: str) -> Path:
    """
    Build an export file path inside the export directory.
    """

    cfg = load_config()

    return cfg.export_dir / filename

def cleanup_export_directory() -> None:
    """
    Remove old exported CSV files from the export directory.
    """

    cfg = load_config()

    export_dir = cfg.export_dir

    if not export_dir.exists():
        return
    
    for file_path in export_dir.glob("*.csv"):
        file_path.unlink(missing_ok=True)

def create_download_button(
        file_path: Path,
        label: str,
        mime: str = "text/csv"
) -> None:
    """
    Create a Streamlit download button for a file.
    """

    if not file_path.exists():
        st.error(f"File not found: {file_path}")
        return
    
    st.success(f"Exported successfully: {file_path}")

    st.download_button(
        label = label,
        data = file_path.read_bytes(),
        file_name = file_path.name,
        mime = mime,
    )

def export_source_csv(
        source_path: Path,
        output_name: str,
        button_label: str,
        download_label: str
) -> None:
    """
    Export a source CSV file to the export directory.
    """

    if not st.button(button_label):
        return
    
    output_path = build_output_path(output_name)

    try:
        with st.spinner("Exporting..."):
            shutil.copyfile(source_path, output_path)
        
        create_download_button(
            file_path = output_path,
            label = download_label
        )
    
    except Exception as error:
        st.error(str(error))

def validate_snr_input(snr_id: str) -> bool:
    """
    Validate SNR ID input.
    """

    if not snr_id:
        st.error("Please enter an SNR ID.")
        return False
    
    if snr_id.lower() == ALL:
        return True

    snr_ids = [s.strip() for s in snr_id.split(",")]

    if not all(s.isdigit() for s in snr_ids):
        st.error(
            "The SNR ID must be a number, "
            "'all' or a comma-separated list of numbers."
        )
        return False

    invalid_ids = [s for s in snr_ids if not snr_exists(s)]

    if invalid_ids:
        st.error(
            "The following SNR IDs were not found: "
            f"{', '.join(invalid_ids)}"
        )
        return False
    
    return True

def get_filter_stages(
        stages: list[str],
        test_type: str
) -> list[str]:
    """
    Filter timeline stages depending on test type.
    """

    if test_type == "gait":
        return stages
    
    return [
        stage
        for stage in stages
        if stage not in GAIT_ONLY_STAGES
    ]

def create_zip_download(
        file_path_1: Path,
        file_path_2: Path
) -> bytes:
    """
    Create a ZIP archive containing two CSV files.
    """

    buffer = BytesIO()

    with zipfile.ZipFile(buffer, "w") as zip_file:
        zip_file.writestr(
            file_path_1.name,
            file_path_1.read_text()
        
        )
        zip_file.writestr(
            file_path_2.name,
            file_path_2.read_text()
        )
    
    return buffer.getvalue()

# ============================================================
# Dashboard
# ============================================================

def render_dashboard() -> None:
    """
    Render dashboard statistics and export buttons.
    """

    st.header("Dashboard")

    col1, col2 = st.columns(2)

    with col1:
        st.metric(
            "Total Patients",
            get_total_patients()
        )

        st.subheader("Timeline stage distribution")

        st.dataframe(
            get_timeline_stages_distribution(),
            width = 'stretch'
        )
    
    with col2:
        st.metric(
            "Patients With IMU Data",
        get_total_patients_with_imu()
        )

        st.subheader("Test type distribution")

        st.dataframe(
            get_test_types_distribution(),
            width = 'stretch'
        )

    cfg = load_config()

    col3, col4 = st.columns(2)

    with col3:
        export_source_csv(
            source_path = cfg.csv_main,
            output_name = "SNR_main.csv",
            button_label = "Export Main CSV",
            download_label = "Download Main CSV"
        )

    with col4:
        export_source_csv(
            source_path = cfg.csv_meta,
            output_name = "SNR_meta.csv",
            button_label = "Export Meta CSV",
            download_label = "Download Meta CSV"
        )

# ============================================================
# Database refresh
# ============================================================

def render_database_refresh() -> None:
    """
    Render database refresh section.
    """

    changed, _, _ = sources_changed()

    if not changed:
        st.success("Database is up to date.")
        return
    
    st.warning(
        "Source data has changed since the last database refresh."
    )

    if st.button("Refresh Database"):
        try:
            with st.spinner("Refreshing database..."):
                refresh_db()

            st.success("Database refreshed successfully.")
            st.rerun()

        except Exception as error:
            st.error(str(error))

# ============================================================
# Reduced CSV exporter
# ============================================================

def render_filter_selection() -> None:
    """
    Render filter selection UI.
    """

    characteristics = get_all_characteristics()

    st.subheader("Filter")

    selected_column = st.selectbox(
        "Column",
        options = characteristics
    )

    selected_value = st.text_input(
        "Value"
    ).strip()

    if selected_column and selected_value:
        if value_exists_objects(selected_column, selected_value):
            st.success("Value exists in database.")
        else:
            st.error("Value not found in database.")
    
    if st.button("Add Filter"):
        st.session_state.filters.append(
            (selected_column, selected_value)
        )

    for index, (column_name, value) in enumerate(
        st.session_state.filters
    ):
        col1, col2 = st.columns([4, 1])

        with col1:
            st.write(f"{column_name} = {value}")
        with col2:
            if st.button("Remove", key = f"filter_{index}"):
                st.session_state.filters.pop(index)
                st.rerun()

def render_column_selection() -> None:
    """
    Render column selection UI.
    """

    characteristics = get_all_characteristics()

    st.subheader("Columns")

    selected_column = st.selectbox(
        "Select Column",
        options = characteristics,
        key = "column_selector"
    )

    if st.button("Add Column"):
        if selected_column not in st.session_state.columns:
            st.session_state.columns.append(selected_column)

    for index, column_name in enumerate(
        st.session_state.columns
    ):
        col1, col2 = st.columns([4, 1])

        with col1:
            st.write(f"{column_name}")

        with col2:
            if st.button("Remove", key = f"column_{index}"):
                st.session_state.columns.pop(index)
                st.rerun()

def render_reduced_csv_exporter() -> None:
    """
    Render reduced CSV export UI.
    """

    st.header("Reduced Size CSV Export")

    render_filter_selection()

    st.divider()

    render_column_selection()

    st.divider()

    if not st.button("Export Reduced CSV"):
        return

    if not st.session_state.columns:
        st.info("No columns selected: all columns will be exported.")
    
    try:
        with st.spinner("Exporting..."):
            output_path = reduced_csv_exporter(
                selected_columns = st.session_state.columns,
                selected_filters = st.session_state.filters
            )

        create_download_button(
            file_path = output_path,
            label = "Download Reduced CSV"
        )
    
    except Exception as error:
        st.error(str(error))

# ============================================================
# IMU CSV exporter
# ============================================================

def render_imu_csv_exporter() -> None:
    """
    Render IMU CSV export UI.
    """

    st.header("IMU CSV Export")

    snr_id = st.text_input("SNR ID", value="").strip()
    
    if not validate_snr_input(snr_id):
        return
    
    available_test_types = get_available_test_types(snr_id)

    test_type = st.selectbox(
        "Test Type",
        options = [ALL] + available_test_types
    )

    available_stages = get_available_stages(snr_id)

    filtered_stages = get_filter_stages(
        stages = available_stages,
        test_type = test_type
    )

    timeline_stage = st.selectbox(
        "Timeline Stage",
        options = [ALL] + filtered_stages
    )

    if not st.button("Export IMU CSV"):
        return
    
    stage_arg = "" if timeline_stage == ALL else timeline_stage
    type_arg = "" if test_type == ALL else test_type

    try:
        with st.spinner("Exporting..."):
            output_path1, output_path2 = imu_csv_export(
                snr_id = snr_id, 
                timeline_stage = stage_arg, 
                test_type = type_arg
            )
    
        if output_path1 == output_path2:
            create_download_button(
                file_path = output_path1,
                label = "Download IMU CSV"
            )

            return

        if not (output_path1.exists() and output_path2.exists()):
            st.error("Export failed.")
            return

        zip_data = create_zip_download(
            file_path_1 = output_path1,
            file_path_2 = output_path2
        )

        st.success("Exported successfully.")

        st.download_button(
            label = "Download IMU CSV",
            data = zip_data,
            file_name = "IMU_export.zip",
            mime="application/zip",
        )

    except Exception as error:
        st.error(str(error))


# ============================================================
# Main
# ============================================================

def main():
    """
    Main Streamlit application entry point.
    """

    initialise_session_state()

    cleanup_export_directory()

    st.title("WideLog IMU CSV Export")

    render_database_refresh()

    tab_dashboard, tab_reduced, tab_imu = st.tabs([
        "Dashboard & Stats",
        "CSV Reduced Exporter",
        "CSV IMU Exporter"
    ])

    with tab_dashboard:
        render_dashboard()
    
    with tab_reduced:
        render_reduced_csv_exporter()

    with tab_imu:
        render_imu_csv_exporter()



if __name__ == "__main__":
    main()
    