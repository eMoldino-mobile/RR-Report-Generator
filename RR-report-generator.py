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

        # --- LOGIC CHANGE ---
        # 1. Create the pure timestamp difference column for export
        df["time_diff_sec"] = df["shot_time"].diff().dt.total_seconds()

        if "ACTUAL CT" in df.columns:
            # Use the pure time_diff_sec for stop logic comparison
            time_diff_sec_calc = df["time_diff_sec"] 
            prev_actual_ct = df["ACTUAL CT"].shift(1)
            rounding_buffer = 2.0
            
            is_a_stop = (prev_actual_ct == 999.9) | (time_diff_sec_calc > (prev_actual_ct + rounding_buffer))
            
            # 2. Create a new internal column for stop/run logic
            df["logic_ct_diff"] = np.where(is_a_stop, time_diff_sec_calc, df["ACTUAL CT"])
        else:
            # If no 'ACTUAL CT', use the pure timestamp diff for logic
            df["logic_ct_diff"] = df["time_diff_sec"]

        # 3. Handle NaNs for both new columns in the first row
        if not df.empty:
            if pd.isna(df.loc[0, "time_diff_sec"]):
                df.loc[0, "time_diff_sec"] = 0 # First shot has no time diff
            
            if pd.isna(df.loc[0, "logic_ct_diff"]):
                # For the first row, logic_ct_diff should be ACTUAL CT if it exists, else 0
                df.loc[0, "logic_ct_diff"] = df.loc[0, "ACTUAL CT"] if "ACTUAL CT" in df.columns else 0
                
        return df
        # --- END LOGIC CHANGE ---

    def _calculate_all_metrics(self) -> dict:
        df = self._prepare_data()
        if df.empty or "ACTUAL CT" not in df.columns:
            return {}

        # --- LOGIC CHANGE: Use 'logic_ct_diff' for all calculations ---
        df_for_mode_calc = df[df["logic_ct_diff"] <= 28800]
        mode_ct = df_for_mode_calc["ACTUAL CT"].mode().iloc[0] if not df_for_mode_calc["ACTUAL CT"].mode().empty else 0
        lower_limit = mode_ct * (1 - self.tolerance)
        upper_limit = mode_ct * (1 + self.tolerance)

        stop_condition = ((df["logic_ct_diff"] < lower_limit) | (df["logic_ct_diff"] > upper_limit)) & (df["logic_ct_diff"] <= 28800)
        df["stop_flag"] = np.where(stop_condition, 1, 0)
        if not df.empty:
            df.loc[0, "stop_flag"] = 0
        df["stop_event"] = (df["stop_flag"] == 1) & (df["stop_flag"].shift(1, fill_value=0) == 0)

        total_shots = len(df)
        stop_events = df["stop_event"].sum()
        downtime_sec = df.loc[df['stop_flag'] == 1, 'logic_ct_diff'].sum()
        production_time_sec = df[df['stop_flag'] == 0]['logic_ct_diff'].sum()

        stop_durations = []
        is_in_stop = False
        current_stop_duration = 0
        for _, row in df.iterrows():
            if row['stop_flag'] == 1:
                is_in_stop = True
                current_stop_duration += row['logic_ct_diff'] # Use logic_ct_diff
            elif is_in_stop and row['stop_flag'] == 0:
                stop_durations.append(current_stop_duration)
                is_in_stop = False
                current_stop_duration = 0
        
        # Add the last stop if the data ends during a stop
        if is_in_stop:
            stop_durations.append(current_stop_duration)
            
        total_downtime_from_stops = sum(stop_durations)
        mttr_sec = total_downtime_from_stops / stop_events if stop_events > 0 else 0

        mtbf_min = (production_time_sec / 60 / stop_events) if stop_events > 0 else (production_time_sec / 60)
        
        total_runtime_sec = (df["shot_time"].max() - df["shot_time"].min()).total_seconds() if total_shots > 1 else 0
        normal_shots = total_shots - df["stop_flag"].sum()
        efficiency = normal_shots / total_shots if total_shots > 0 else 0
        
        first_stop_index = df[df['stop_event']].index.min()
        time_to_first_dt_sec = df.loc[:first_stop_index-1, 'logic_ct_diff'].sum() if pd.notna(first_stop_index) and first_stop_index > 0 else production_time_sec
        avg_cycle_time = production_time_sec / normal_shots if normal_shots > 0 else 0
        
        df["run_group"] = df["stop_event"].cumsum()
        run_durations = df[df["stop_flag"] == 0].groupby("run_group")["logic_ct_diff"].sum().div(60).reset_index(name="duration_min")
        # --- END LOGIC CHANGE ---

        return {
            "processed_df": df, "mode_ct": mode_ct, "lower_limit": lower_limit, "upper_limit": upper_limit,
            "total_shots": total_shots, "efficiency": efficiency, "stop_events": stop_events,
            "normal_shots": normal_shots, "mttr_min": mttr_sec / 60, "mtbf_min": mtbf_min,
            "production_run_sec": total_runtime_sec, "tot_down_time_sec": downtime_sec,
            "time_to_first_dt_min": time_to_first_dt_sec / 60,
            "avg_cycle_time_sec": avg_cycle_time,
            "run_durations": run_durations
        }

# --- Excel Generation Function (FIXED) ---
def generate_excel_report(all_runs_data, tolerance):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # --- Define Formats ---
        header_format = workbook.add_format({'bold': True, 'bg_color': '#002060', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        sub_header_format = workbook.add_format({'bold': True, 'bg_color': '#C5D9F1', 'border': 1})
        label_format = workbook.add_format({'bold': True, 'align': 'left'})
        percent_format = workbook.add_format({'num_format': '0.0%', 'border': 1})
        time_format = workbook.add_format({'num_format': '[h]:mm:ss', 'border': 1})
        mins_format = workbook.add_format({'num_format': '0.00 "min"', 'border': 1})
        secs_format = workbook.add_format({'num_format': '0.00 "sec"', 'border': 1})
        data_format = workbook.add_format({'border': 1})
        datetime_format = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss', 'border': 1})
        error_format = workbook.add_format({'bold': True, 'font_color': 'red'})

        # --- Generate a Sheet for Each Run ---
        for run_id, data in all_runs_data.items():
            ws = workbook.add_worksheet(f"Run_{run_id:03d}")
            df_run = data['processed_df'].copy()
            start_row = 19 # The row where the data table starts (1-indexed)
            
            # --- Dynamically find column letters ---
            col_map = {name: chr(ord('A') + i) for i, name in enumerate(df_run.columns)}
            
            # Columns for Header Formulas
            stop_col = col_map.get('STOP')
            stop_event_col = col_map.get('STOP EVENT')
            time_bucket_col = col_map.get('TIME BUCKET')
            first_col = 'A' # Always use the first column for counting total shots
            
            # Columns for Table Formulas
            time_diff_col_dyn = col_map.get('TIME DIFF SEC')
            cum_count_col_dyn = col_map.get('CUMULATIVE COUNT')
            run_dur_col_dyn = col_map.get('RUN DURATION')
            bucket_col_dyn = col_map.get('TIME BUCKET')
            shot_time_col_dyn = col_map.get('SHOT TIME') # <-- Get SHOT TIME column

            # Helper column will be the one *after* the last data column
            data_cols_count = len(df_run.columns)
            helper_col_letter = chr(ord('A') + data_cols_count)
            ws.set_column(f'{helper_col_letter}:{helper_col_letter}', None, None, {'hidden': True})
            
            # --- Define Analysis Block Columns ---
            # Start 2 columns after the helper column (leaves one blank column)
            analysis_start_col_idx = data_cols_count + 2 
            analysis_col_1 = chr(ord('A') + analysis_start_col_idx)     # Bucket #
            analysis_col_2 = chr(ord('A') + analysis_start_col_idx + 1) # Duration Range
            analysis_col_3 = chr(ord('A') + analysis_start_col_idx + 2) # Events Count

            # Check for missing essential columns
            missing_cols = []
            if not stop_col: missing_cols.append('STOP')
            if not stop_event_col: missing_cols.append('STOP EVENT')
            if not time_bucket_col: missing_cols.append('TIME BUCKET')
            if not time_diff_col_dyn: missing_cols.append('TIME DIFF SEC')
            if not cum_count_col_dyn: missing_cols.append('CUMULATIVE COUNT')
            if not run_dur_col_dyn: missing_cols.append('RUN DURATION')
            if not shot_time_col_dyn: missing_cols.append('SHOT TIME') # <-- Add check
            
            if missing_cols:
                ws.write('A5', f"Error: Missing columns for formulas: {', '.join(missing_cols)}", error_format)
            
            table_formulas_ok = all(item is not None for item in [
                stop_col, stop_event_col, time_diff_col_dyn, 
                cum_count_col_dyn, run_dur_col_dyn, bucket_col_dyn,
                shot_time_col_dyn # <-- Add to check
            ])

            # --- Layout ---
            ws.merge_range('A1:B1', data['equipment_code'], header_format)
            ws.write('A2', 'Date', label_format)
            ws.write('B2', f"{data['start_time'].strftime('%Y-%m-%d')} to {data['end_time'].strftime('%Y-%m-%d')}")
            ws.write('A3', 'Method', label_format)
            ws.write('B3', 'Every Shot')

            ws.write('E1', 'Mode CT', sub_header_format)
            ws.write('E2', data['mode_ct'], secs_format)

            ws.write('F1', 'Outside L1', sub_header_format); ws.write('G1', 'Outside L2', sub_header_format); ws.write('H1', 'IDLE', sub_header_format)
            ws.write('F2', 'Lower Limit', label_format); ws.write('G2', 'Upper Limit', label_format); ws.write('H2', 'Stops', label_format)
            
            ws.write_formula('F3', f'=E2*(1-{tolerance})', secs_format)
            ws.write_formula('G3', f'=E2*(1+{tolerance})', secs_format)
            if stop_col:
                ws.write_formula('H3', f"=SUM({stop_col}{start_row}:{stop_col}{start_row + len(df_run) - 1})", sub_header_format)
            else:
                ws.write('H3', 'N/A', sub_header_format)


            ws.write('K1', 'Total Shot Count', label_format); ws.write('L1', 'Normal Shot Count', label_format)
            ws.write_formula('K2', f"=COUNTA({first_col}{start_row}:{first_col}{start_row + len(df_run) - 1})", sub_header_format)
            ws.write_formula('L2', f"=K2-H3", sub_header_format)
            
            ws.write('K4', 'Efficiency', label_format); ws.write('L4', 'Stop Events', label_format)
            ws.write_formula('K5', f"=L2/K2", percent_format)
            if stop_event_col:
                ws.write_formula('L5', f"=SUM({stop_event_col}{start_row}:{stop_event_col}{start_row + len(df_run) - 1})", sub_header_format)
            else:
                ws.write('L5', 'N/A', sub_header_format)

            ws.write('F5', 'Tot Run Time', label_format); ws.write('G5', 'Tot Down Time', label_format)
            ws.write('F6', data['production_run_sec'] / 86400, time_format)
            ws.write('G6', data['tot_down_time_sec'] / 86400, time_format)
            ws.write_formula('F7', f"=(F6-G6)/F6", percent_format); ws.write_formula('G7', f"=G6/F6", percent_format)
            
            ws.merge_range('K8:L8', 'Reliability Metrics', header_format)
            ws.write('K9', 'MTTR (Avg)', label_format); ws.write('L9', data['mttr_min'], mins_format)
            ws.write('K10', 'MTBF (Avg)', label_format); ws.write('L10', data['mtbf_min'], mins_format)
            
            ws.write('K11', 'Time to First DT', label_format)
            # --- FORMULA CHANGE for Time to First DT ---
            if stop_event_col and run_dur_col_dyn:
                end_row_num = start_row + len(df_run) - 1
                match_range = f'{stop_event_col}{start_row}:{stop_event_col}{end_row_num}'
                index_range = f'{run_dur_col_dyn}:{run_dur_col_dyn}'
                # Find the first row with stop_event=1, get its row number, then INDEX into the RUN DURATION column
                # (18 + MATCH(...)) gives the absolute row number.
                # IFERROR handles the case of no stops, falling back to the pre-calculated total run time.
                formula = f'IFERROR(INDEX({index_range}, 18 + MATCH(1, {match_range}, 0)) * 1440, {data["time_to_first_dt_min"]})'
                ws.write_formula('L11', formula, mins_format)
            else:
                # Fallback to static value if columns are missing
                ws.write('L11', data['time_to_first_dt_min'], mins_format) 
            # --- END FORMULA CHANGE ---
            
            ws.write('K12', 'Avg Cycle Time', label_format); ws.write('L12', data['avg_cycle_time_sec'], secs_format)

            # --- Time Bucket Analysis (Dynamically Placed) ---
            ws.merge_range(f'{analysis_col_1}14:{analysis_col_3}14', 'Time Bucket Analysis', header_format)
            ws.write(f'{analysis_col_1}15', 'Bucket', sub_header_format)
            ws.write(f'{analysis_col_2}15', 'Duration Range', sub_header_format)
            ws.write(f'{analysis_col_3}15', 'Events Count', sub_header_format)
            
            max_bucket = 20
            for i in range(1, max_bucket + 1):
                ws.write(f'{analysis_col_1}{15+i}', i, sub_header_format)
                ws.write(f'{analysis_col_2}{15+i}', f"{(i-1)*20} - {i*20} min", sub_header_format)
                if time_bucket_col:
                    ws.write_formula(f'{analysis_col_3}{15+i}', f'=COUNTIF({time_bucket_col}:{time_bucket_col},{i})', sub_header_format)
                else:
                    ws.write(f'{analysis_col_3}{15+i}', 'N/A', sub_header_format)

            ws.write(f'{analysis_col_2}{16+max_bucket}', 'Grand Total', sub_header_format)
            ws.write_formula(f'{analysis_col_3}{16+max_bucket}', f"=SUM({analysis_col_3}16:{analysis_col_3}{15+max_bucket})", sub_header_format)

            # --- Data Table ---
            ws.write_row('A18', df_run.columns, header_format)
            
            if 'SHOT TIME' in df_run.columns:
                df_run['SHOT TIME'] = pd.to_datetime(df_run['SHOT TIME']).dt.tz_localize(None)
            df_run.fillna('', inplace=True)
            
            # Write the entire DataFrame first
            for i, row in enumerate(df_run.to_numpy()):
                current_row_excel_idx = start_row + i
                for c_idx, value in enumerate(row):
                    col_name = df_run.columns[c_idx]
                    if col_name in ['CUMULATIVE COUNT', 'RUN DURATION', 'TIME BUCKET']:
                        continue 
                    
                    # --- FORMULA CHANGE for TIME DIFF SEC ---
                    if col_name == 'TIME DIFF SEC':
                        if table_formulas_ok:
                            if i == 0: # First data row
                                ws.write_number(current_row_excel_idx - 1, c_idx, 0, secs_format)
                            else: # Subsequent data rows
                                current_row_num_excel = current_row_excel_idx # e.g., 19, 20...
                                prev_row_num_excel = current_row_excel_idx - 1 # e.g., 18, 19...
                                # Excel row numbers are 1-based, so 19 is correct for first data row
                                formula = f'=({shot_time_col_dyn}{current_row_num_excel}-{shot_time_col_dyn}{prev_row_num_excel})*86400'
                                ws.write_formula(current_row_excel_idx - 1, c_idx, formula, secs_format)
                        else:
                            # Fallback if columns are missing
                            ws.write(current_row_excel_idx - 1, c_idx, value, secs_format)
                        continue # Move to the next cell
                    # --- END FORMULA CHANGE ---
                        
                    
                    if isinstance(value, pd.Timestamp):
                        ws.write_datetime(current_row_excel_idx - 1, c_idx, value, datetime_format)
                    elif isinstance(value, (bool, np.bool_)):
                        ws.write_number(current_row_excel_idx - 1, c_idx, int(value), data_format)
                    else:
                        ws.write(current_row_excel_idx - 1, c_idx, value, data_format)
            
            # --- Write Dynamic Table Formulas ---
            if table_formulas_ok:
                for i in range(len(df_run)):
                    row_num = start_row + i
                    prev_row = row_num - 1
                    
                    # Helper column for run duration sum
                    # This formula now correctly references the 'TIME DIFF SEC' column
                    # which itself contains a formula. Excel handles this dependency.
                    if i == 0:
                        helper_formula = f'=IF({stop_col}{row_num}=0, {time_diff_col_dyn}{row_num}, 0)'
                    else:
                        helper_formula = f'=IF({stop_event_col}{row_num}=1, 0, {helper_col_letter}{prev_row}) + IF({stop_col}{row_num}=0, {time_diff_col_dyn}{row_num}, 0)'
                    ws.write_formula(f'{helper_col_letter}{row_num}', helper_formula)

                    # CUMULATIVE COUNT
                    cum_count_formula = f'=COUNTIF(${stop_event_col}${start_row}:${stop_event_col}{row_num},1) & "/" & IF({stop_event_col}{row_num}=1, "0 sec", TEXT({helper_col_letter}{row_num}/86400, "[h]:mm:ss"))'
                    ws.write_formula(f'{cum_count_col_dyn}{row_num}', cum_count_formula, data_format)

                    # --- FIX: Allow RUN DURATION and TIME BUCKET on first row ---
                    # RUN DURATION
                    if i == 0:
                        run_dur_formula = f'=IF({stop_event_col}{row_num}=1, 0, "")' # Special case for first row: duration is 0
                    else:
                        run_dur_formula = f'=IF({stop_event_col}{row_num}=1, {helper_col_letter}{prev_row}/86400, "")'
                    ws.write_formula(f'{run_dur_col_dyn}{row_num}', run_dur_formula, time_format)

                    # TIME BUCKET
                    if i == 0:
                         time_bucket_formula = f'=IF({stop_event_col}{row_num}=1, IFERROR(FLOOR(0/60/20, 1) + 1, ""), "")' # Special case for first row
                    else:
                        time_bucket_formula = f'=IF({stop_event_col}{row_num}=1, IFERROR(FLOOR({helper_col_letter}{prev_row}/60/20, 1) + 1, ""), "")'
                    ws.write_formula(f'{bucket_col_dyn}{row_num}', time_bucket_formula, data_format)
                    # --- END FIX ---
            else:
                if cum_count_col_dyn:
                    ws.write(f'{cum_count_col_dyn}{start_row-1}', "Formula Error", error_format)


            # Auto-fit columns
            for i, col_name in enumerate(df_run.columns):
                try:
                    width = max(
                        len(str(col_name)), 
                        df_run[col_name].astype(str).map(len).max()
                    )
                except Exception:
                    width = len(str(col_name)) # Fallback
                
                ws.set_column(i, i, width + 2 if width < 40 else 40)

    return output.getvalue()

# --- Streamlit App UI ---
st.title("⚙️ Run-Based Excel Report Generator")
st.info("This tool processes a raw run-rate file, identifies individual production runs, and generates a detailed, multi-sheet Excel report with formula-linked cells.")

uploaded_file = st.file_uploader("Upload a Run Rate Excel file", type=["xlsx", "xls"])

if uploaded_file:
    try:
        df_raw = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        st.stop()
        
    
    if "EQUIPMENT CODE" in df_raw.columns: df_raw.rename(columns={"EQUIPMENT CODE": "tool_id"}, inplace=True)
    if "TOOLING ID" in df_raw.columns: df_raw.rename(columns={"TOOLING ID": "tool_id"}, inplace=True)
    
    if "tool_id" not in df_raw.columns or ("SHOT TIME" not in df_raw.columns and not {"YEAR", "MONTH", "DAY", "TIME"}.issubset(df_raw.columns)):
        st.error("The uploaded file must contain 'EQUIPMENT CODE' (or 'TOOLING ID') and a valid timestamp column ('SHOT TIME' or YEAR/MONTH/DAY/TIME).")
    else:
        st.sidebar.header("Report Parameters")
        tolerance = st.sidebar.slider("Tolerance Band (% of Mode CT)", 0.01, 0.20, 0.05, 0.01, format="%.2f")
        run_interval_hours = st.sidebar.slider("Run Interval Threshold (hours)", 1, 24, 8, 1)

        if st.button("Generate Excel Report", use_container_width=True, type="primary"):
            with st.spinner("Processing data and building report..."):
                try:
                    base_calc = RunRateCalculator(df_raw, tolerance)
                    df_processed = base_calc.results.get("processed_df", pd.DataFrame())

                    if df_processed.empty:
                        st.error("Could not process data. Check file format or data. 'ACTUAL CT' column might be missing or all timestamp data might be invalid.")
                    else:
                        split_col = 'logic_ct_diff' if 'logic_ct_diff' in df_processed.columns else 'time_diff_sec'
                        is_new_run = df_processed[split_col] > (run_interval_hours * 3600)
                        df_processed['run_id'] = is_new_run.cumsum()

                        all_runs_data = {}
                        desired_columns_base = [
                            'SUPPLIER NAME', 'tool_id', 'SESSION ID', 'SHOT ID', 'shot_time',
                            'APPROVED CT', 'ACTUAL CT', 
                            'time_diff_sec', 'stop_flag', 'stop_event', 'run_group'
                        ]
                        
                        formula_columns = ['CUMULATIVE COUNT', 'RUN DURATION', 'TIME BUCKET']

                        for run_id, df_run_raw in df_processed.groupby('run_id'):
                            run_calculator = RunRateCalculator(df_run_raw.copy(), tolerance)
                            run_results = run_calculator.results
                            
                            if not run_results:
                                st.warning(f"Could not process Run ID {run_id}. Skipping.")
                                continue

                            run_results['equipment_code'] = df_run_raw['tool_id'].iloc[0]
                            run_results['start_time'] = df_run_raw['shot_time'].min()
                            run_results['end_time'] = df_run_raw['shot_time'].max()

                            export_df = run_results['processed_df'].copy()
                            
                            for col in formula_columns:
                                if col not in export_df:
                                    export_df[col] = ''
                            
                            columns_to_export = [col for col in desired_columns_base if col in export_df.columns]
                            columns_to_export.extend(formula_columns)
                            
                            final_export_df = export_df[columns_to_export].rename(columns={
                                'tool_id': 'EQUIPMENT CODE', 'shot_time': 'SHOT TIME',
                                'time_diff_sec': 'TIME DIFF SEC', 'stop_flag': 'STOP', 'stop_event': 'STOP EVENT'
                            })

                            final_desired_renamed = [
                                'SUPLIER NAME', 'EQUIPMENT CODE', 'SESSION ID', 'SHOT ID', 'SHOT TIME',
                                'APPROVED CT', 'ACTUAL CT', 
                                'TIME DIFF SEC', 'STOP', 'STOP EVENT', 'run_group',
                                'CUMULATIVE COUNT', 'RUN DURATION', 'TIME BUCKET'
                            ]
                            
                            for col in final_desired_renamed:
                                if col not in final_export_df.columns:
                                    final_export_df[col] = ''
                            
                            final_export_df = final_export_df[[col for col in final_desired_renamed if col in final_export_df.columns]]

                            run_results['processed_df'] = final_export_df
                            all_runs_data[run_id] = run_results
                        
                        if not all_runs_data:
                            st.error("No valid runs were processed.")
                        else:
                            excel_data = generate_excel_report(all_runs_data, tolerance)
                            st.success("✅ Report generated successfully!")
                            st.download_button(
                                label="📥 Download Excel Report",
                                data=excel_data,
                                file_name=f"Run_Based_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True
                            )
                except Exception as e:
                    st.error(f"An unexpected error occurred during processing: {e}")
                    import traceback
                    st.exception(traceback.format_exc())

else:
    st.info("Upload an Excel file to begin.")

