from __future__ import annotations
from pathlib import Path
import streamlit as st
import duckdb

# Local imports
from widelog.config import load_config
from widelog.imu_csv_export import imu_csv_export

st.set_page_config(page_title="WideLog IMU CSV Export", layout="centered")

def build_output_path(out_csv: str) -> Path:
    cfg = load_config()
    return Path(cfg.out_dir) / out_csv

def main():
    st.title("WideLog IMU CSV Export (MVP)")

    cfg = load_config()
    con = duckdb.connect(cfg.duckdb_path, read_only = True)
    out_csv = ""

    # Inputs
    snr_id = st.text_input("SNR ID", value="").strip()
    
    # ID check
    if not snr_id:
        st.error("Please enter an SNR ID to load available options.")
        return
    
    # Frontend, check if id is a number
    if not snr_id.isdigit():
        st.error("The SNR ID must be a number")
        st.stop()

    # Backend, check if id is in the DB
    exists = con.execute("""
        SELECT 1
        FROM objects_with_imu
        WHERE snr_id = ?
        LIMIT 1;
    """, [snr_id]).fetchone()

    if exists is None:
        st.error(f"The SNR {snr_id} not found in the database")
        st.stop()
    

    # DROPDOWN Menu timeline and types
    stages = con.execute("""
        SELECT DISTINCT timeline_stage
        FROM objects_with_imu
        WHERE snr_id = ?
            AND timeline_stage IS NOT NULL
        ORDER BY timeline_stage
    """, [snr_id]).fetchall()

    types = con.execute("""
        SELECT DISTINCT test_type
        FROM objects_with_imu
        WHERE snr_id = ?
            AND test_type IS NOT NULL
        ORDER BY test_type
    """, [snr_id]).fetchall()

    stages = [s[0] for s in stages]
    types = [t[0] for t in types]

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

        try:
            with st.spinner("Exporting..."):
                out_csv = imu_csv_export(
                    snr_id=snr_id, 
                    timeline_stage=timeline_stage, 
                    test_type=test_type
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
    