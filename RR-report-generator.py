import streamlit as st
import pandas as pd
import numpy as np
import xlsxwriter
from io import BytesIO
from datetime import datetime

# --- Page Configuration ---
st.set_page_config(
    layout="centered",
    page_title="Run-Based Excel Report Generator"
)

# --- Core Calculation Class (Adapted from your original code) ---
# This class contains the essential logic for processing the data.
class RunRateCalculator:
    def __init__(self, df: pd.DataFrame, tolerance: float):
        self.df_raw = df.copy()
        self.tolerance = tolerance
        self.results = self._calculate_all_metrics()

    def _prepare_data(self) -> pd.DataFrame:
        df = self.df_raw.copy()
        if {"YEAR", "MONTH", "DAY", "TIME"}.issubset(df.columns):
            datetime_str = df["YEAR"].astype(str) + "-" + df["MONTH"].astype(str) + "-" + df["DAY"].astype(str) + " " + df['TIME'].astype(str)
            df["shot_time"] = pd.to_datetime(datetime_str, errors="coerce")
        elif "SHOT TIME" in df.columns:
            df["shot_time"] = pd.to_datetime(df["SHOT TIME"], errors="coerce")
        else:
            return pd.DataFrame()

        df = df.dropna(subset=["shot_time"]).sort_values("shot_time").reset_index(drop=True)
        if df.empty: return pd.DataFrame()

        if "ACTUAL CT" in df.columns:
            time_diff_sec = df["shot_time"].diff().dt.total_seconds()
            prev_actual_ct = df["ACTUAL CT"].shift(1)
            rounding_buffer = 2.0
            use_timestamp_diff = (prev_actual_ct == 999.9) | (time_diff_sec > (prev_actual_ct + rounding_buffer))
            df["ct_diff_sec"] = np.where(use_timestamp_diff, time_diff_sec, prev_actual_ct)
        else:
            df["ct_diff_sec"] = df["shot_time"].diff().dt.total_seconds()

        if not df.empty and pd.isna(df.loc[0, "ct_diff_sec"]):
            df.loc[0, "ct_diff_sec"] = df.loc[0, "ACTUAL CT"] if "ACTUAL CT" in df.columns else 0
        return df

    def _calculate_all_metrics(self) -> dict:
        df = self._prepare_data()
        if df.empty or "ACTUAL CT" not in df.columns:
            return {}

        df_for_mode_calc = df[df["ct_diff_sec"] <= 28800]
        mode_ct = df_for_mode_calc["ACTUAL CT"].mode().iloc[0] if not df_for_mode_calc["ACTUAL CT"].mode().empty else 0
        lower_limit = mode_ct * (1 - self.tolerance)
        upper_limit = mode_ct * (1 + self.tolerance)

        stop_condition = ((df["ct_diff_sec"] < lower_limit) | (df["ct_diff_sec"] > upper_limit)) & (df["ct_diff_sec"] <= 28800)
        df["stop_flag"] = np.where(stop_condition, 1, 0)
        if not df.empty:
            df.loc[0, "stop_flag"] = 0
        df["stop_event"] = (df["stop_flag"] == 1) & (df["stop_flag"].shift(1, fill_value=0) == 0)

        total_shots = len(df)
        stop_events = df["stop_event"].sum()
        downtime_sec = df.loc[df['stop_flag'] == 1, 'ct_diff_sec'].sum()
        production_time_sec = df[df['stop_flag'] == 0]['ct_diff_sec'].sum()

        # Corrected MTTR calculation
        stop_durations = []
        is_in_stop = False
        current_stop_duration = 0
        for _, row in df.iterrows():
            if row['stop_flag'] == 1:
                is_in_stop = True
                current_stop_duration += row['ct_diff_sec']
            elif is_in_stop and row['stop_flag'] == 0:
                stop_durations.append(current_stop_duration)
                is_in_stop = False
                current_stop_duration = 0
        
        total_downtime_from_stops = sum(stop_durations)
        mttr_sec = total_downtime_from_stops / stop_events if stop_events > 0 else 0

        mtbf_min = (production_time_sec / 60 / stop_events) if stop_events > 0 else (production_time_sec / 60)
        
        total_runtime_sec = (df["shot_time"].max() - df["shot_time"].min()).total_seconds() if total_shots > 1 else 0
        normal_shots = total_shots - df["stop_flag"].sum()
        efficiency = normal_shots / total_shots if total_shots > 0 else 0
        
        # Calculate Time to First DT (Downtime)
        first_stop_index = df[df['stop_event']].index.min()
        time_to_first_dt_sec = df.loc[:first_stop_index-1, 'ct_diff_sec'].sum() if pd.notna(first_stop_index) and first_stop_index > 0 else production_time_sec
        avg_cycle_time = production_time_sec / normal_shots if normal_shots > 0 else 0
        
        df["run_group"] = df["stop_event"].cumsum()
        run_durations = df[df["stop_flag"] == 0].groupby("run_group")["ct_diff_sec"].sum().div(60).reset_index(name="duration_min")

        return {
            "processed_df": df, "mode_ct": mode_ct, "lower_limit": lower_limit, "upper_limit": upper_limit,
            "total_shots": total_shots, "efficiency": efficiency, "stop_events": stop_events,
            "normal_shots": normal_shots, "mttr_min": mttr_sec / 60, "mtbf_min": mtbf_min,
            "production_run_sec": total_runtime_sec, "tot_down_time_sec": downtime_sec,
            "time_to_first_dt_min": time_to_first_dt_sec / 60,
            "avg_cycle_time_sec": avg_cycle_time,
            "run_durations": run_durations
        }

# --- Excel Generation Function ---
def generate_excel_report(all_runs_data):
    output = BytesIO()
    # Added nan_inf_to_errors to prevent corruption from invalid numbers
    with pd.ExcelWriter(output, engine='xlsxwriter', options={'nan_inf_to_errors': True}) as writer:
        workbook = writer.book
        
        # --- Define Formats ---
        header_format = workbook.add_format({'bold': True, 'bg_color': '#002060', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        sub_header_format = workbook.add_format({'bold': True, 'bg_color': '#C5D9F1', 'border': 1})
        label_format = workbook.add_format({'bold': True, 'align': 'left'})
        data_format = workbook.add_format({'border': 1})
        percent_format = workbook.add_format({'num_format': '0.0%', 'border': 1})
        time_format = workbook.add_format({'num_format': 'd "d" h "h" m "m" s "s"', 'border': 1})
        mins_format = workbook.add_format({'num_format': '0.00 "min"', 'border': 1})
        secs_format = workbook.add_format({'num_format': '0.00 "sec"', 'border': 1})
        # --- FIX: Added a specific datetime format ---
        datetime_format = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss', 'border': 1})


        # --- Generate a Sheet for Each Run ---
        for run_id, data in all_runs_data.items():
            ws = workbook.add_worksheet(f"Run_{run_id:03d}")
            df_run = data['processed_df'].copy() # Use a copy to avoid modifying original
            
            # --- Layout inspired by screenshots ---
            
            # A: General Info
            ws.merge_range('A1:B1', data['equipment_code'], header_format)
            ws.write('A2', 'Date', label_format)
            ws.write('B2', f"{data['start_time'].strftime('%Y-%m-%d')} to {data['end_time'].strftime('%Y-%m-%d')}")
            ws.write('A3', 'Method', label_format)
            ws.write('B3', 'Every Shot')

            # F,G,H: Limits and Stops
            ws.write('F1', 'Outside L1', sub_header_format)
            ws.write('G1', 'Outside L2', sub_header_format)
            ws.write('H1', 'IDLE', sub_header_format)
            ws.write('F2', 'Lower Limit', label_format)
            ws.write('G2', 'Upper Limit', label_format)
            ws.write('H2', 'Stops', label_format)
            ws.write('F3', data['lower_limit'], secs_format)
            ws.write('G3', data['upper_limit'], secs_format)
            ws.write_formula('H3', f"=SUMIF(J:J, 1)", data_format) # Safely sum stops column

            # K,L: Shot Counts
            ws.write('K1', 'Total Shot Count', label_format)
            ws.write('L1', 'Normal Shot Count', label_format)
            ws.write_formula('K2', f"=COUNTA(A19:A{19+len(df_run)})", data_format) # Count any value in first column
            ws.write_formula('L2', f"=K2-H3", data_format)
            
            # K,L: Efficiency and Stop Events
            ws.write('K4', 'Efficiency', label_format)
            ws.write('L4', 'Stop Events', label_format)
            ws.write_formula('K5', f"=L2/K2", percent_format)
            ws.write('L5', data['stop_events'], data_format)

            # F,G: Time Metrics
            ws.write('F5', 'Tot Run Time', label_format)
            ws.write('G5', 'Tot Down Time', label_format)
            ws.write('F6', data['production_run_sec'] / 86400, time_format) # Convert seconds to Excel time format
            ws.write('G6', data['tot_down_time_sec'] / 86400, time_format)
            ws.write_formula('F7', f"=(F6-G6)/F6", percent_format) # Uptime %
            ws.write_formula('G7', f"=G6/F6", percent_format) # Downtime %
            
            # K,L: Reliability Metrics
            ws.merge_range('K8:L8', 'Reliability Metrics', header_format)
            ws.write('K9', 'MTTR (Avg)', label_format)
            ws.write('K10', 'MTBF (Avg)', label_format)
            ws.write('K11', 'Time to First DT (Avg)', label_format)
            ws.write('K12', 'Avg Cycle Time (Avg)', label_format)
            ws.write('L9', data['mttr_min'], mins_format)
            ws.write('L10', data['mtbf_min'], mins_format)
            ws.write('L11', data['time_to_first_dt_min'], mins_format)
            ws.write('L12', data['avg_cycle_time_sec'], secs_format)

            # Time Bucket Analysis
            ws.merge_range('A14:C14', 'Time Bucket Analysis', header_format)
            ws.write('A15', 'Time Bucket', sub_header_format)
            ws.write('B15', 'Stop Events Count', sub_header_format)
            
            bucket_counts = data['run_durations']['duration_min'].apply(lambda x: int(x // 20) + 1).value_counts().sort_index()
            
            for i in range(1, 11):
                ws.write(f'A{15+i}', i, data_format)
                count = bucket_counts.get(i, 0)
                ws.write(f'B{15+i}', count, data_format)
            ws.write(f'A26', 'Grand Total', sub_header_format)
            ws.write_formula(f'B26', f"=SUM(B16:B25)", sub_header_format)
            
            # Raw Data Table
            ws.write_row('A18', df_run.columns, header_format)
            
            # --- FIX: Set column formats and widths BEFORE writing data ---
            for i, col_name in enumerate(df_run.columns):
                width = max(len(str(col_name)), df_run[col_name].astype(str).map(len).max())
                final_width = width + 2 if width < 40 else 40
                
                if col_name == 'SHOT TIME':
                    ws.set_column(i, i, final_width, datetime_format)
                else:
                    ws.set_column(i, i, final_width)

            start_row = 19
            # Convert datetime to timezone-unaware for ExcelWriter compatibility
            if 'SHOT TIME' in df_run.columns and pd.api.types.is_datetime64_any_dtype(df_run['SHOT TIME']):
                df_run['SHOT TIME'] = df_run['SHOT TIME'].dt.tz_localize(None)
                
            # Fill NaN values to prevent potential issues
            df_run.fillna('', inplace=True)

            for i, row in enumerate(df_run.to_numpy()):
                current_row_num = start_row + i
                ws.write_row(f'A{current_row_num}', row)

            # --- Add Formulas to Raw Data Table ---
            # Find column letters for formula linking
            try:
                shot_time_col = chr(ord('A') + df_run.columns.get_loc('SHOT TIME'))
                actual_ct_col = chr(ord('A') + df_run.columns.get_loc('ACTUAL CT'))
                time_diff_col = chr(ord('A') + df_run.columns.get_loc('TIME DIFF SEC'))
                stop_col = chr(ord('A') + df_run.columns.get_loc('STOP'))
                cum_count_col = chr(ord('A') + df_run.columns.get_loc('CUMULATIVE COUNT'))
                run_dur_col = chr(ord('A') + df_run.columns.get_loc('RUN DURATION'))
                bucket_col = chr(ord('A') + df_run.columns.get_loc('TIME BUCKET'))
            except KeyError:
                # If a column for formulas is missing, we can't add them.
                st.warning(f"Could not add all formulas to Excel sheet '{ws.name}' due to missing columns in source file.")
                continue # Skip to next run sheet

            for i in range(len(df_run)):
                row_num = 19 + i
                # TIME DIFF SEC
                if i == 0:
                    ws.write_formula(f'{time_diff_col}{row_num}', f'={actual_ct_col}{row_num}', data_format)
                else:
                    ws.write_formula(f'{time_diff_col}{row_num}', f'=({shot_time_col}{row_num}-{shot_time_col}{row_num-1})*86400', data_format)
                
                # CUMULATIVE COUNT (FIXED FORMULA)
                formula = f'=COUNTIF(${stop_col}$19:${stop_col}{row_num},1) & "/" & TEXT(SUM(${time_diff_col}$19:${time_diff_col}{row_num})/86400, "[h]:mm:ss")'
                ws.write_formula(f'{cum_count_col}{row_num}', formula, data_format)

                # RUN DURATION
                ws.write_formula(f'{run_dur_col}{row_num}', f'=SUM(${time_diff_col}$19:${time_diff_col}{row_num})/86400', time_format) # Convert seconds to excel day fraction

                # TIME BUCKET
                ws.write_formula(f'{bucket_col}{row_num}', f'=IFERROR(FLOOR({run_dur_col}{row_num}*1440/20,1)+1, "")', data_format)

    return output.getvalue()

# --- Streamlit App UI ---
st.title("⚙️ Run-Based Excel Report Generator")

st.info("This tool processes a raw run-rate file, identifies individual production runs, and generates a detailed, multi-sheet Excel report with formula-linked cells.")

uploaded_file = st.file_uploader("Upload a Run Rate Excel file", type=["xlsx", "xls"])

if uploaded_file:
    df_raw = pd.read_excel(uploaded_file)
    
    # Standardize column names
    if "EQUIPMENT CODE" in df_raw.columns:
        df_raw.rename(columns={"EQUIPMENT CODE": "tool_id"}, inplace=True)
    if "TOOLING ID" in df_raw.columns:
        df_raw.rename(columns={"TOOLING ID": "tool_id"}, inplace=True)
    
    if "tool_id" not in df_raw.columns or ("SHOT TIME" not in df_raw.columns and not {"YEAR", "MONTH", "DAY", "TIME"}.issubset(df_raw.columns)):
        st.error("The uploaded file must contain 'EQUIPMENT CODE' (or 'TOOLING ID') and a valid timestamp column ('SHOT TIME' or YEAR/MONTH/DAY/TIME).")
    else:
        st.sidebar.header("Report Parameters")
        tolerance = st.sidebar.slider(
            "Tolerance Band (% of Mode CT)", 0.01, 0.20, 0.05, 0.01,
            help="Defines the ±% around the Mode Cycle Time to identify a stop event."
        )
        run_interval_hours = st.sidebar.slider(
            "Run Interval Threshold (hours)", 1, 24, 8, 1,
            help="Maximum idle time (in hours) between shots before a new Production Run is identified."
        )

        if st.button("Generate Excel Report", use_container_width=True, type="primary"):
            with st.spinner("Processing data and building report..."):
                # 1. Initial data preparation
                base_calc = RunRateCalculator(df_raw, tolerance)
                df_processed = base_calc.results.get("processed_df", pd.DataFrame())

                if df_processed.empty:
                    st.error("Could not process data. Check file format or data.")
                else:
                    # 2. Identify individual runs
                    is_new_run = df_processed['ct_diff_sec'] > (run_interval_hours * 3600)
                    df_processed['run_id'] = is_new_run.cumsum()

                    # 3. Calculate metrics for each individual run
                    all_runs_data = {}
                    # Define the ideal set of columns we want in the final report
                    desired_columns = [
                        'SUPPLIER NAME', 'tool_id', 'SESSION ID', 'SHOT ID', 'shot_time',
                        'APPROVED CT', 'ACTUAL CT', 'CT MIN', 'ct_diff_sec', 'stop_flag',
                        'CUMULATIVE COUNT', 'RUN DURATION', 'TIME BUCKET'
                    ]

                    for run_id, df_run in df_processed.groupby('run_id'):
                        run_calculator = RunRateCalculator(df_run.copy(), tolerance)
                        run_results = run_calculator.results
                        
                        # Add some extra info for the report
                        run_results['equipment_code'] = df_run['tool_id'].iloc[0]
                        run_results['start_time'] = df_run['shot_time'].min()
                        run_results['end_time'] = df_run['shot_time'].max()

                        # Prepare the DataFrame for export
                        export_df = run_results['processed_df'].copy()
                        export_df['CUMULATIVE COUNT'] = '' # Placeholder for formula
                        export_df['RUN DURATION'] = ''     # Placeholder for formula
                        export_df['TIME BUCKET'] = ''      # Placeholder for formula
                        
                        # Check which of our desired columns actually exist in the processed data
                        columns_to_export = [col for col in desired_columns if col in export_df.columns]
                        
                        # Use only the existing columns for the final DataFrame
                        run_results['processed_df'] = export_df[columns_to_export].rename(columns={
                            'tool_id': 'EQUIPMENT CODE',
                            'shot_time': 'SHOT TIME',
                            'ct_diff_sec': 'TIME DIFF SEC',
                            'stop_flag': 'STOP'
                        })

                        all_runs_data[run_id] = run_results
                    
                    # 4. Generate the Excel file
                    excel_data = generate_excel_report(all_runs_data)

                    st.success("✅ Report generated successfully!")

                    st.download_button(
                        label="📥 Download Excel Report",
                        data=excel_data,
                        file_name=f"Run_Based_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )

