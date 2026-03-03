from __future__ import annotations
from pathlib import Path
import streamlit as st

# Local imports
from widelog.config import load_config
from widelog.imu_csv_export import imu_csv_export

st.set_page_config(page_title="WideLog IMU CSV Export", layout="centered")

def build_output_path(snr_id: str, timeline_stage: str, test_type: str) -> Path:
    cfg = load_config()
    out_csv = f"SNR{snr_id}_{timeline_stage}_{test_type}.csv"
    return Path(cfg.out_dir) / out_csv

def main():
    st.title("WideLog IMU CSV Export (MVP)")

    cfg = load_config()

    snr_id = st.text_input("SNR ID", value="").strip()
    timeline_stage = st.text_input("Timeline Stage (optional)", value="").strip()
    test_type = st.text_input("Test Type (optional)", value="").strip()

    if st.button("Export IMU CSV"):
        if not snr_id:
            st.error("Please enter a valid SNR ID.")
            return
        with st.spinner("Exporting..."):
            imu_csv_export(snr_id=snr_id, timeline_stage=timeline_stage, test_type=test_type)
        
        out_path = build_output_path(snr_id, timeline_stage, test_type)
        
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

if __name__ == "__main__":
    main()
    