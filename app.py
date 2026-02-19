import streamlit as st
import pandas as pd
import sqlite3
import time
import re
from io import BytesIO
from datetime import datetime
import altair as alt

# CONFIGURATION
st.set_page_config(page_title="Mapua Library Audit System", layout="wide", page_icon="📚")

# STYLING
st.markdown("""
    <style>
    .stApp { background-color: #F8F9FA; font-family: 'Segoe UI', sans-serif; }
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #CE1126 0%, #8B0000 100%); color: white; }
    [data-testid="stSidebar"] * { color: white !important; }
    div[data-testid="metric-container"] { background-color: white; border: 1px solid #ddd; border-radius: 8px; padding: 15px; border-left: 5px solid #CE1126; box-shadow: 2px 2px 5px rgba(0,0,0,0.05); }
    div.stButton > button { background-color: #CE1126; color: white; border-radius: 5px; font-weight: bold; border: none; }
    .chart-container { background-color: white; border-radius: 8px; padding: 15px; border: 1px solid #ddd; margin-bottom: 20px; box-shadow: 2px 2px 5px rgba(0,0,0,0.05); }
    </style>
    """, unsafe_allow_html=True)

DB_FILE = "library_mdm.db"

# --- DATABASE ENGINE ---
def get_db_connection():
    return sqlite3.connect(DB_FILE, timeout=10)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS raw_resource (
                    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Accession_Number TEXT, ResourceID TEXT, ISBN TEXT, Title TEXT, Author TEXT,
                    Classification TEXT, Location TEXT, Department TEXT, System_Status TEXT, Physical_Status TEXT,
                    Copyright_Year INTEGER, Subject_Heading TEXT, System_Count INTEGER, Physical_Count INTEGER, Remarks TEXT,
                    Raw_Prefix TEXT
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS golden_resource (
                    golden_id INTEGER PRIMARY KEY,
                    master_id TEXT, master_isbn TEXT, master_title TEXT, master_author TEXT,
                    master_department TEXT, master_accession_list TEXT, master_classification TEXT, master_category TEXT,
                    System_Status TEXT, Physical_Status TEXT, Copyright_Year INTEGER, master_subject TEXT,
                    System_Count INTEGER, Physical_Count INTEGER, Remarks TEXT
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS audit_log (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    action TEXT, details TEXT
                )''')
    conn.commit()
    conn.close()

def log_action(action, details):
    try:
        conn = get_db_connection()
        conn.execute("INSERT INTO audit_log (action, details) VALUES (?, ?)", (action, details))
        conn.commit()
        conn.close()
    except: pass

def clean_text_for_matching(text):
    if pd.isna(text): return ""
    return re.sub(r'[^a-zA-Z0-9]', '', str(text)).lower()

def extract_department_from_remarks(remark):
    val = str(remark).strip()
    if not val or val.lower() == 'nan': return 'Unassigned'
    exclusion_pattern = re.compile(r'(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|aug|sept|oct|nov|dec|\d{4}|encoded|purchased|donation|donated|mstacks|pull|reserve)', re.IGNORECASE)
    if exclusion_pattern.search(val): return 'Unassigned'
    if len(val) > 15: return 'Unassigned'
    return val.upper()

#  ACTIVE INVENTORY IMPORT ENGINE
def process_excel_automated(uploaded_file, progress_bar, status_text):
    start_time = time.time()
    all_dfs = []
    debug_log = []
    
    try:
        xls = pd.ExcelFile(uploaded_file)
        target_sheets = [s for s in xls.sheet_names if "active" in s.lower()]
    except Exception as e: 
        return pd.DataFrame(), f"Error reading file: {str(e)}", []

    if not target_sheets:
        return pd.DataFrame(), "System Error: No 'Active' sheet found in the uploaded file.", []

    total_sheets = len(target_sheets)
    
    for idx, sheet in enumerate(target_sheets):
        progress_bar.progress((idx / total_sheets) * 0.8)
        status_text.markdown(f"**⚡ Processing Data from: {sheet}**")

        try:
            df_raw = pd.read_excel(xls, sheet_name=sheet, header=None)
            header_idx = -1
            search_zone = df_raw.head(50).astype(str).apply(lambda x: x.str.lower())
            
            for i, row in search_zone.iterrows():
                row_str = row.values.astype(str)
                if any('title' in x for x in row_str) and (any('call' in x for x in row_str) or any('acc' in x for x in row_str)):
                    header_idx = i
                    break
            
            if header_idx != -1:
                raw_headers = df_raw.iloc[header_idx].fillna('Unknown').astype(str).str.strip().tolist()
                
                if raw_headers[0] in ['Unknown', 'nan', '']:
                    raw_headers[0] = 'Raw_Prefix'
                
                seen = {}
                unique_headers = []
                for h in raw_headers:
                    if h in seen:
                        seen[h] += 1
                        unique_headers.append(f"{h}_{seen[h]}")
                    else:
                        seen[h] = 0
                        unique_headers.append(h)
                
                df_raw.columns = unique_headers
                df = df_raw[header_idx+1:].copy()
            else:
                debug_log.append(f"Skipped '{sheet}': No valid header row identified.")
                continue 
            
            col_map = {}
            for col in df.columns:
                c = str(col).lower().strip()
                if c == 'raw_prefix': col_map[col] = 'Raw_Prefix'
                elif c in ['book title', 'title']: col_map[col] = 'Title'
                elif c in ['call number']: col_map[col] = 'ResourceID'
                elif c in ['author']: col_map[col] = 'Author'
                elif c in ['acc. no.', 'accession number']: col_map[col] = 'Accession_Number'
                elif c in ['copyright', 'copyright year']: col_map[col] = 'Copyright_Year'
                elif c in ['location']: col_map[col] = 'Location'
                elif c in ['inventory remarks']: col_map[col] = 'System_Status' 
                elif c in ['remarks']: col_map[col] = 'Remarks'
                elif c in ['subject heading']: col_map[col] = 'Subject_Heading'
            
            df = df.rename(columns=col_map)
            valid_cols = [col for col in col_map.values() if col in df.columns]
            df = df[valid_cols]
            df = df.loc[:, ~df.columns.duplicated()]
            
            if 'Title' in df.columns:
                df = df.dropna(subset=['Title'])
                df['Source_Sheet'] = sheet
                all_dfs.append(df)
                debug_log.append(f"Successfully loaded {len(df)} records from '{sheet}'.")
                
        except Exception as e:
            debug_log.append(f"Error reading '{sheet}': {e}")
            continue

    if not all_dfs: 
        return pd.DataFrame(), "No valid inventory data extracted.", debug_log

    status_text.text("🤖 Applying classification rules (Reading Column 1)...")
    df_final = pd.concat(all_dfs, ignore_index=True)
    
    if 'Location' not in df_final.columns: df_final['Location'] = ''
    df_final['Location'] = df_final['Location'].astype(str).replace('nan', '').str.strip()
    
    mask_blank_loc = (df_final['Location'] == '') | (df_final['Location'].str.lower() == 'unknown')
    
    if 'Raw_Prefix' in df_final.columns and mask_blank_loc.any():
        prefix_s = df_final['Raw_Prefix'].astype(str).str.upper()
        df_final.loc[mask_blank_loc & prefix_s.str.contains('FIL'), 'Location'] = 'Filipiniana'
        df_final.loc[mask_blank_loc & prefix_s.str.contains('REF'), 'Location'] = 'Reference'
        df_final.loc[mask_blank_loc & prefix_s.str.contains('FIC'), 'Location'] = 'Fiction'
        df_final.loc[mask_blank_loc & prefix_s.str.contains('CIR'), 'Location'] = 'Circulation'
    
    mask_blank_loc = (df_final['Location'] == '') | (df_final['Location'].str.lower() == 'unknown')
    
    if mask_blank_loc.any():
        if 'ResourceID' in df_final.columns:
            call_s = df_final['ResourceID'].astype(str).str.upper()
            df_final.loc[mask_blank_loc & call_s.str.contains('FIL'), 'Location'] = 'Filipiniana'
            df_final.loc[mask_blank_loc & (call_s.str.contains('REF') | call_s.str.contains('RES')), 'Location'] = 'Reference'
            df_final.loc[mask_blank_loc & call_s.str.contains('FIC'), 'Location'] = 'Fiction'
            df_final.loc[mask_blank_loc & call_s.str.contains('CIR'), 'Location'] = 'Circulation'
            
        df_final.loc[df_final['Location'] == '', 'Location'] = 'General Collection'

    df_final['Classification'] = df_final['Location']
    
    if 'Remarks' in df_final.columns:
        df_final['Department'] = df_final['Remarks'].apply(extract_department_from_remarks)
    else:
        df_final['Department'] = 'Unassigned'
        df_final['Remarks'] = ''

    if 'Accession_Number' in df_final.columns:
        df_final['Accession_Number'] = df_final['Accession_Number'].fillna('Not Yet Delivered')

    req_cols = ['Accession_Number', 'ResourceID', 'ISBN', 'Title', 'Author', 'Classification', 'Location', 'Department', 'Copyright_Year', 'System_Count', 'Physical_Count', 'System_Status', 'Remarks', 'Subject_Heading', 'Raw_Prefix']
    for c in req_cols:
        if c not in df_final.columns: df_final[c] = ''
    
    df_final['System_Count'] = 1
    df_final['Physical_Count'] = 0
    df_final = df_final[req_cols]
    df_final = df_final.loc[:, ~df_final.columns.duplicated()]
    
    progress_bar.progress(1.0)
    return df_final, f"Successfully processed {len(df_final)} books.", debug_log

init_db()

# --- SIDEBAR ---
try: st.sidebar.image("mapua_logo.png", use_container_width=True)
except: st.sidebar.header("📚 Library Audit")

menu = st.sidebar.radio("Navigation", ["Dashboard", "Import & Validate", "Data Processing", "Audit Scanner", "Masterlist", "System Admin"])
st.sidebar.divider()

if st.sidebar.button("🔄 Reset Database"):
    conn = get_db_connection()
    conn.close()
    try: open(DB_FILE, 'w').close() 
    except: pass
    init_db()
    st.sidebar.success("Database reset successfully.")
    time.sleep(1)
    st.rerun()

# 1. DASHBOARD
if menu == "Dashboard":
    st.title("📊 Library Analytics Dashboard")
    conn = get_db_connection()
    try: df = pd.read_sql("SELECT * FROM golden_resource", conn)
    except: df = pd.DataFrame()
    conn.close()

    if df.empty:
        st.info("System Ready. Please proceed to the 'Import & Validate' tab to upload the inventory file.")
    else:
        df['Copyright_Year'] = pd.to_numeric(df['Copyright_Year'], errors='coerce').fillna(0)
        curr_year = datetime.now().year
        cutoff = curr_year - 5
        
        if 'master_id' in df.columns:
            df['Subject_Class'] = df['master_id'].str[0].str.upper().apply(lambda x: x if str(x).isalpha() else 'Unclassified')
        else:
            df['Subject_Class'] = 'Unclassified'
        
        total = int(df['System_Count'].sum())
        found = int(df['Physical_Count'].sum())
        missing_vols = total - found
        recent = int(df[df['Copyright_Year'] >= cutoff]['System_Count'].sum())
        
        # TOP METRICS
        c1, c2, c3 = st.columns(3)
        c1.metric("📚 Unique Titles", f"{len(df):,}")
        c2.metric("📦 Total Volumes (Expected)", f"{total:,}")
        c3.metric("✨ Recency (Last 5 Years)", f"{recent:,}", "Accreditation Compliant")
        
        st.divider()

        #  SMART INSIGHTS
        st.subheader("💡 Smart Library Insights")
        weeding_df = df[(df['Copyright_Year'] > 0) & (df['Copyright_Year'] < curr_year - 15)].copy()
        weeding_candidates = len(weeding_df)
        
        pending_df = df[df['master_accession_list'].str.contains('Not Yet Delivered', case=False, na=False)]
        pending_titles = len(pending_df)
        pending_volumes = pending_df['System_Count'].sum() if not pending_df.empty else 0
        
        col_insight1, col_insight2 = st.columns(2)
        col_insight1.info(f"**🗑️ Weeding Consideration:** You have **{weeding_candidates}** titles older than 15 years. Review for potential weeding.")
        col_insight2.warning(f"**🚚 Pending Deliveries:** **{pending_volumes}** volumes are missing accession numbers and may still be in transit.")
        
        st.divider()
        
        # ADVANCED GRAPHS SECTION 
        st.subheader("📈 Collection Analytics")
        
        # ROW 1 OF GRAPHS
        col_A, col_B = st.columns(2)
        with col_A:
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            st.write("##### 🏫 Volumes by Department")
            if 'master_department' in df.columns:
                dept_counts = df[df['master_department'] != 'Unassigned']['master_department'].value_counts().head(10).reset_index()
                dept_counts.columns = ['Department', 'Volumes']
                chart_dept = alt.Chart(dept_counts).mark_bar(color='#CE1126').encode(
                    x=alt.X('Volumes:Q', title='Total Volumes'),
                    y=alt.Y('Department:N', sort='-x', title='')
                ).properties(height=300)
                st.altair_chart(chart_dept, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col_B:
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            st.write("##### 📖 Volumes by Call Class")
            call_counts = df['Subject_Class'].value_counts().head(10).reset_index()
            call_counts.columns = ['Subject Class', 'Volumes']
            chart_call = alt.Chart(call_counts).mark_bar(color='#FFC72C').encode(
                x=alt.X('Subject Class:N', sort='-y', title='LC Class'),
                y=alt.Y('Volumes:Q', title='Total Volumes')
            ).properties(height=300)
            st.altair_chart(chart_call, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        # ROW 2 OF GRAPHS
        col_C, col_D = st.columns(2)
        with col_C:
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            st.write("##### 📅 Collection Age Profile (Copyright Year)")
            valid_years = df[(df['Copyright_Year'] > 1900) & (df['Copyright_Year'] <= curr_year + 1)]
            year_counts = valid_years.groupby('Copyright_Year')['System_Count'].sum().reset_index()
            chart_age = alt.Chart(year_counts).mark_area(
                color=alt.Gradient(
                    gradient='linear',
                    stops=[alt.GradientStop(color='#8B0000', offset=0), alt.GradientStop(color='white', offset=1)],
                    x1=1, x2=1, y1=1, y2=0
                ),
                line={'color':'#8B0000'}
            ).encode(
                x=alt.X('Copyright_Year:O', title='Copyright Year'),
                y=alt.Y('System_Count:Q', title='Volumes'),
                tooltip=['Copyright_Year', 'System_Count']
            ).properties(height=300)
            st.altair_chart(chart_age, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with col_D:
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            st.write("##### 🎯 Audit Progress (Status)")
            audit_data = pd.DataFrame({
                'Status': ['Verified (Found)', 'Unverified (Missing)'],
                'Volumes': [found, missing_vols]
            })
            chart_audit = alt.Chart(audit_data).mark_arc(innerRadius=50).encode(
                theta=alt.Theta(field="Volumes", type="quantitative"),
                color=alt.Color(field="Status", type="nominal", scale=alt.Scale(domain=['Verified (Found)', 'Unverified (Missing)'], range=['#28a745', '#dc3545'])),
                tooltip=['Status', 'Volumes']
            ).properties(height=300)
            st.altair_chart(chart_audit, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
            
        # ROW 3 OF GRAPHS (TOP 5 DUPLICATES)
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.write("##### 📚 Top 5 Most Duplicated Books")
        top_dupes = df.nlargest(5, 'System_Count')[['master_title', 'System_Count']]
        # Truncate long titles for display
        top_dupes['Short_Title'] = top_dupes['master_title'].apply(lambda x: (x[:45] + '...') if len(str(x)) > 45 else x)
        
        chart_dupes = alt.Chart(top_dupes).mark_bar(color='#6c757d').encode(
            x=alt.X('System_Count:Q', title='Number of Copies (Volumes)'),
            y=alt.Y('Short_Title:N', sort='-x', title='Book Title'),
            tooltip=['master_title', 'System_Count']
        ).properties(height=250)
        st.altair_chart(chart_dupes, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.divider()
        st.subheader("📥 Actionable Reports & Exports")
        
        col_btn1, col_btn2, col_btn3 = st.columns(3)
        
        #  EXPORT 1: OFFICIAL REPORT
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            wb = writer.book
            fmt_title = wb.add_format({'bold': True, 'font_size': 14, 'font_color': '#8B0000', 'valign': 'vcenter'})
            fmt_header = wb.add_format({'bold': True, 'bg_color': '#8B0000', 'font_color': 'white', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
            fmt_cell = wb.add_format({'border': 1, 'valign': 'vcenter'})
            fmt_cell_center = wb.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
            fmt_grand_total = wb.add_format({'bold': True, 'bg_color': '#FFC72C', 'font_color': 'black', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
            
            s_summ = wb.add_worksheet("SUMMARY")
            s_summ.write(0, 0, "MAPUA UNIVERSITY LIBRARY - OVERALL COLLECTION SUMMARY", fmt_title)
            s_summ.write(2, 0, "Classification", fmt_header)
            s_summ.write(2, 1, "Titles", fmt_header)
            s_summ.write(2, 2, "Volumes", fmt_header)
            s_summ.set_column('A:A', 35)
            s_summ.set_column('B:C', 20)
            
            summ_data = df.groupby('master_classification').agg(Titles=('golden_id', 'count'), Volumes=('System_Count', 'sum')).reset_index()
            row_idx = 3
            for i, r in summ_data.iterrows():
                s_summ.write(row_idx, 0, str(r['master_classification']).upper(), fmt_cell)
                s_summ.write(row_idx, 1, r['Titles'], fmt_cell_center)
                s_summ.write(row_idx, 2, r['Volumes'], fmt_cell_center)
                row_idx += 1
                
            s_summ.write(row_idx, 0, "NOT YET DELIVERED", fmt_cell)
            s_summ.write(row_idx, 1, pending_titles, fmt_cell_center)
            s_summ.write(row_idx, 2, pending_volumes, fmt_cell_center)
            row_idx += 1
            
            s_summ.write(row_idx, 0, "GRAND TOTAL", fmt_grand_total)
            s_summ.write(row_idx, 1, len(df), fmt_grand_total)
            s_summ.write(row_idx, 2, df['System_Count'].sum(), fmt_grand_total)

            s_loc = wb.add_worksheet("Summary By Location")
            s_loc.write(0, 0, "MAPUA UNIVERSITY LIBRARY - LOCATION SUMMARY", fmt_title)
            s_loc.write(2, 0, "Location", fmt_header)
            s_loc.write(2, 1, "Titles", fmt_header)
            s_loc.write(2, 2, "Volumes", fmt_header)
            s_loc.set_column('A:A', 30)
            s_loc.set_column('B:C', 18)
            
            loc_summ = df.groupby('master_classification').agg(Titles=('golden_id', 'count'), Volumes=('System_Count', 'sum')).reset_index()
            for i, r in loc_summ.iterrows():
                s_loc.write(i+3, 0, r['master_classification'], fmt_cell)
                s_loc.write(i+3, 1, r['Titles'], fmt_cell_center)
                s_loc.write(i+3, 2, r['Volumes'], fmt_cell_center)

            s_dept = wb.add_worksheet("Summary By Department")
            s_dept.write(0, 0, "MAPUA UNIVERSITY LIBRARY - DEPARTMENT SUMMARY", fmt_title)
            s_dept.write(2, 0, "Department", fmt_header)
            s_dept.write(2, 1, "Titles", fmt_header)
            s_dept.write(2, 2, "Volumes", fmt_header)
            s_dept.set_column('A:A', 30)
            s_dept.set_column('B:C', 18)
            
            if 'master_department' in df.columns:
                dept_summ = df.groupby('master_department').agg(Titles=('golden_id', 'count'), Volumes=('System_Count', 'sum')).reset_index()
                for i, r in dept_summ.iterrows():
                    s_dept.write(i+3, 0, r['master_department'], fmt_cell)
                    s_dept.write(i+3, 1, r['Titles'], fmt_cell_center)
                    s_dept.write(i+3, 2, r['Volumes'], fmt_cell_center)

            s_call = wb.add_worksheet("Summary By Call Number")
            s_call.write(0, 0, "MAPUA UNIVERSITY LIBRARY - CALL NUMBER SUMMARY", fmt_title)
            s_call.write(2, 0, "Call Number (Class)", fmt_header)
            s_call.write(2, 1, "Titles", fmt_header)
            s_call.write(2, 2, "Volumes", fmt_header)
            s_call.set_column('A:A', 30)
            s_call.set_column('B:C', 18)
            
            call_summ = df.groupby('Subject_Class').agg(Titles=('golden_id', 'count'), Volumes=('System_Count', 'sum')).reset_index()
            for i, r in call_summ.iterrows():
                s_call.write(i+3, 0, r['Subject_Class'], fmt_cell_center)
                s_call.write(i+3, 1, r['Titles'], fmt_cell_center)
                s_call.write(i+3, 2, r['Volumes'], fmt_cell_center)
                    
            s2 = wb.add_worksheet("Accreditation Profile")
            years = list(range(curr_year, cutoff-1, -1))
            headers = ["Collection"] + [str(y) for y in years] + ["Older", "Total Volumes"]
            for col, h in enumerate(headers): s2.write(0, col, h, fmt_header)
            
            df['Year_Col'] = df['Copyright_Year'].apply(lambda x: x if x in years else 'Older')
            piv = df.pivot_table(index='master_classification', columns='Year_Col', values='System_Count', aggfunc='sum', fill_value=0)
            
            r_idx = 1
            for loc in piv.index:
                s2.write(r_idx, 0, loc, fmt_cell)
                c_idx = 1
                row_tot = 0
                for y in years:
                    val = piv.loc[loc, y] if y in piv.columns else 0
                    s2.write(r_idx, c_idx, val, fmt_cell_center)
                    row_tot += val
                    c_idx += 1
                val_old = piv.loc[loc, 'Older'] if 'Older' in piv.columns else 0
                s2.write(r_idx, c_idx, val_old, fmt_cell_center)
                s2.write(r_idx, c_idx+1, row_tot + val_old, fmt_cell_center)
                r_idx += 1

            excel_export_df = df.rename(columns={
                'master_accession_list': 'Acc. No.',
                'master_id': 'Call Number',
                'master_title': 'Book Title',
                'master_author': 'Author',
                'Copyright_Year': 'Copyright',
                'master_classification': 'Location',
                'System_Status': 'Inventory Remarks',
                'Remarks': 'Remarks',
                'master_subject': 'Subject Heading'
            })
            
            display_cols = [
                'Acc. No.', 'Call Number', 'Book Title', 'Author', 
                'Copyright', 'Location', 'Inventory Remarks', 'Remarks', 
                'Subject Heading', 'Physical_Status', 'System_Count', 'Physical_Count'
            ]
            display_cols = [col for col in display_cols if col in excel_export_df.columns]
            export_data = excel_export_df[display_cols]
            
            s_master = wb.add_worksheet("Active_Masterlist")
            
            for col_num, value in enumerate(export_data.columns.values):
                s_master.write(0, col_num, value, fmt_header)
                
            for row_num, row_data in enumerate(export_data.values):
                for col_num, cell_value in enumerate(row_data):
                    if col_num in [0, 4, 5, 10, 11]: 
                        s_master.write(row_num + 1, col_num, cell_value, fmt_cell_center)
                    else:
                        s_master.write(row_num + 1, col_num, cell_value, fmt_cell)
            
            s_master.set_column('A:A', 15) 
            s_master.set_column('B:B', 25) 
            s_master.set_column('C:C', 55) 
            s_master.set_column('D:D', 30) 
            s_master.set_column('E:E', 12) 
            s_master.set_column('F:F', 20) 
            s_master.set_column('G:G', 25) 
            s_master.set_column('H:H', 25) 
            s_master.set_column('I:I', 35) 
            s_master.set_column('J:J', 18) 
            s_master.set_column('K:L', 15) 
            s_master.freeze_panes(1, 0)
            
        with col_btn1:
            st.download_button("📄 Official Accreditation Report", output.getvalue(), f"Mapua_Audit_Report_{datetime.now().strftime('%Y%m%d')}.xlsx")

        # EXPORT 2: MISSING BOOKS
        missing_output = BytesIO()
        missing_books = df[df['Physical_Count'] == 0].copy()
        missing_books.sort_values(by=['master_classification', 'master_id'], inplace=True)
        
        with pd.ExcelWriter(missing_output, engine='xlsxwriter') as writer:
            mb_clean = missing_books[['master_classification', 'master_id', 'master_title', 'master_author', 'master_accession_list']].rename(
                columns={'master_classification': 'Location', 'master_id': 'Call Number', 'master_title': 'Title', 'master_author': 'Author', 'master_accession_list': 'Accession No.'}
            )
            mb_sheet = writer.book.add_worksheet("Missing_Books")
            
            fmt_header_m = wb.add_format({'bold': True, 'bg_color': '#8B0000', 'font_color': 'white', 'border': 1, 'align': 'center'})
            fmt_cell_m = wb.add_format({'border': 1})
            
            for col_num, value in enumerate(mb_clean.columns.values):
                mb_sheet.write(0, col_num, value, fmt_header_m)
            
            for row_num, row_data in enumerate(mb_clean.values):
                for col_num, cell_value in enumerate(row_data):
                    mb_sheet.write(row_num + 1, col_num, cell_value, fmt_cell_m)
                    
            mb_sheet.set_column('A:A', 20)
            mb_sheet.set_column('B:B', 25)
            mb_sheet.set_column('C:C', 50)
            mb_sheet.set_column('D:D', 30)
            mb_sheet.set_column('E:E', 25)
            mb_sheet.freeze_panes(1, 0)

        with col_btn2:
            st.download_button("🔍 Search & Recovery List", missing_output.getvalue(), f"Missing_Books_{datetime.now().strftime('%Y%m%d')}.xlsx")

        # EXPORT 3: WEEDING CANDIDATES
        weeding_output = BytesIO()
        weeding_df.sort_values(by=['Copyright_Year', 'master_classification'], inplace=True)
        
        with pd.ExcelWriter(weeding_output, engine='xlsxwriter') as writer:
            wd_clean = weeding_df[['master_classification', 'master_id', 'master_title', 'master_author', 'Copyright_Year', 'master_accession_list']].rename(
                columns={'master_classification': 'Location', 'master_id': 'Call Number', 'master_title': 'Title', 'master_author': 'Author', 'Copyright_Year': 'Copyright', 'master_accession_list': 'Accession No.'}
            )
            wd_sheet = writer.book.add_worksheet("Weeding_Candidates")
            
            for col_num, value in enumerate(wd_clean.columns.values):
                wd_sheet.write(0, col_num, value, fmt_header_m)
            
            for row_num, row_data in enumerate(wd_clean.values):
                for col_num, cell_value in enumerate(row_data):
                    wd_sheet.write(row_num + 1, col_num, cell_value, fmt_cell_m)
                    
            wd_sheet.set_column('A:A', 20)
            wd_sheet.set_column('B:B', 25)
            wd_sheet.set_column('C:C', 50)
            wd_sheet.set_column('D:D', 30)
            wd_sheet.set_column('E:E', 15)
            wd_sheet.set_column('F:F', 25)
            wd_sheet.freeze_panes(1, 0)

        with col_btn3:
            st.download_button("🗑️ Weeding Candidates List", weeding_output.getvalue(), f"Weeding_List_{datetime.now().strftime('%Y%m%d')}.xlsx")

# 2. IMPORT & AUTO-SORT
elif menu == "Import & Validate":
    st.header("📥 Data Import & Validation")
    st.info("System Protocol: Upload the official Library Inventory Excel file. The system will automatically map data and identify classification tags.")
    
    up = st.file_uploader("Upload Inventory File (.xlsx)", type=["xlsx"])
    if up and st.button("🚀 Process Data"):
        bar = st.progress(0)
        status = st.empty()
        df, msg, debug_logs = process_excel_automated(up, bar, status)
        
        with st.expander("📝 View System Logs", expanded=False):
            for log in debug_logs:
                if "⚠️" in log or "Error" in log: st.warning(log)
                else: st.success(log)

        if not df.empty:
            conn = get_db_connection()
            try: old = pd.read_sql("SELECT * FROM raw_resource", conn)
            except: old = pd.DataFrame()
            
            if not old.empty and 'Accession_Number' in df.columns:
                new_accs = df[df['Accession_Number'] != 'Not Yet Delivered']['Accession_Number'].unique()
                kept = old[~old['Accession_Number'].isin(new_accs)]
                final = pd.concat([kept, df], ignore_index=True)
            else:
                final = df
            
            final.loc[:, ~final.columns.duplicated()].to_sql("raw_resource", conn, if_exists='replace', index=False)
            log_action("System Import", f"Processed {len(df)} inventory records.")
            conn.close()
            st.success(f"✅ {msg}")
            st.dataframe(df.head())
        else:
            st.error(msg)

# 3. CLEANUP
elif menu == "Data Processing":
    st.header("⚙️ Data Standardization & Deduplication")
    conn = get_db_connection()
    count = pd.read_sql("SELECT COUNT(*) FROM raw_resource", conn).iloc[0][0]
    st.metric("Raw Records in Staging", count)
    
    if st.button("▶️ Execute Data Standardization"):
        raw = pd.read_sql("SELECT * FROM raw_resource", conn)
        
        if not raw.empty:
            expected_cols = ['Subject_Heading', 'Remarks', 'Physical_Status', 'System_Status', 
                             'Copyright_Year', 'ISBN', 'ResourceID', 'Author', 'Title', 'Accession_Number', 'Location', 'Classification', 'Department']
            for col in expected_cols:
                if col not in raw.columns: raw[col] = 'Unassigned'
            
            raw['key'] = raw.apply(lambda x: clean_text_for_matching(str(x['Title']) + str(x['Author'])), axis=1)
            
            gold = []
            for k, g in raw.groupby('key'):
                m = g.iloc[0]
                accs = ", ".join(g['Accession_Number'].astype(str).unique())
                gold.append({
                    'master_title': m['Title'], 'master_author': m['Author'], 'master_id': m['ResourceID'],
                    'master_isbn': m['ISBN'], 'master_accession_list': accs,
                    'master_classification': m['Location'], 
                    'master_category': m['Location'],
                    'master_department': m['Department'], 
                    'System_Status': m['System_Status'], 
                    'Physical_Status': m['Physical_Status'],
                    'Copyright_Year': m['Copyright_Year'], 'System_Count': len(g), 'Physical_Count': 0,
                    'master_subject': m['Subject_Heading'], 
                    'Remarks': m['Remarks'] 
                })
            pd.DataFrame(gold).to_sql("golden_resource", conn, if_exists='replace', index=True, index_label="golden_id")
            log_action("Data Processing", "Executed deduplication sequence.")
            st.success(f"✅ Protocol Complete. Condensed {len(raw)} raw records into {len(gold)} master titles.")
        else:
            st.warning("No staging data available for processing.")
    conn.close()

# 4. SCANNER
elif menu == "Audit Scanner":
    st.header(" Physical Audit Scanner")
    
    tab1, tab2 = st.tabs([" Single Entry Scan", " Continuous / Batch Scan"])
    
    with tab1:
        st.markdown("Utilize this module for singular manual verification.")
        with st.form("scan_form", clear_on_submit=True):
            code = st.text_input("Barcode Input", placeholder="Scan Accession Number, Call Number, or ISBN")
            if st.form_submit_button("✅ Verify Record"):
                if code:
                    conn = get_db_connection()
                    cur = conn.cursor()
                    q = "SELECT golden_id, master_title, Physical_Count, master_classification, master_accession_list FROM golden_resource WHERE master_id=? OR master_isbn=? OR master_accession_list LIKE ?"
                    cur.execute(q, (code, code, f"%{code}%"))
                    res = cur.fetchone()
                    if res:
                        new_c = res[2] + 1
                        cur.execute("UPDATE golden_resource SET Physical_Count=? WHERE golden_id=?", (new_c, res[0]))
                        conn.commit()
                        log_msg = f"Verified: [{code}] - {res[1]} | Location: {res[3]}"
                        log_action("Audit Scan", log_msg)
                        st.success(f"✅ RECORD VALIDATED: {res[1]} (Location: {res[3]})")
                    else:
                        st.error("❌ RECORD NOT FOUND. Item does not exist in the active masterlist.")
                    conn.close()

    with tab2:
        st.markdown("Utilize this module for rapid, high-volume wireless scanning.")
        batch_data = st.text_area("Data Buffer (One barcode per line)", height=200, placeholder="ACC-001\nACC-002\nACC-003...")
        
        if st.button("🚀 Execute Batch Process"):
            if batch_data.strip():
                codes = [c.strip() for c in batch_data.split('\n') if c.strip()]
                conn = get_db_connection()
                cur = conn.cursor()
                
                success_count = 0
                failed_codes = []
                
                progress_batch = st.progress(0)
                for idx, c in enumerate(codes):
                    q = "SELECT golden_id, master_title, Physical_Count, master_classification FROM golden_resource WHERE master_id=? OR master_isbn=? OR master_accession_list LIKE ?"
                    cur.execute(q, (c, c, f"%{c}%"))
                    res = cur.fetchone()
                    if res:
                        new_c = res[2] + 1
                        cur.execute("UPDATE golden_resource SET Physical_Count=? WHERE golden_id=?", (new_c, res[0]))
                        success_count += 1
                        log_msg = f"Verified: [{c}] - {res[1]} | Location: {res[3]}"
                        log_action("Batch Audit", log_msg)
                    else:
                        failed_codes.append(c)
                    
                    progress_batch.progress((idx + 1) / len(codes))
                
                conn.commit()
                conn.close()
                
                st.success(f"✅ Batch operations complete. {success_count} / {len(codes)} records successfully verified.")
                if failed_codes:
                    st.warning(f"⚠️ Action Required: {len(failed_codes)} barcodes were not recognized in the database:")
                    st.write(", ".join(failed_codes))
            else:
                st.error("Buffer is empty. Please input barcode data.")
    
    st.divider()
    st.subheader("System Access Logs")
    conn = get_db_connection()
    st.dataframe(pd.read_sql("SELECT timestamp, details FROM audit_log WHERE action IN ('Audit Scan', 'Batch Audit') ORDER BY timestamp DESC LIMIT 5", conn), use_container_width=True)
    conn.close()

# 5. MASTERLIST
elif menu == "Masterlist":
    st.header(" Master Database Directory")
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM golden_resource", conn)
        excel_view = df.rename(columns={
            'master_accession_list': 'Acc. No.',
            'master_id': 'Call Number',
            'master_title': 'Book Title',
            'master_author': 'Author',
            'Copyright_Year': 'Copyright',
            'master_classification': 'Location',
            'System_Status': 'Inventory Remarks',
            'Remarks': 'Remarks',
            'master_subject': 'Subject Heading'
        })
        
        display_cols = [
            'Acc. No.', 'Call Number', 'Book Title', 'Author', 
            'Copyright', 'Location', 'Inventory Remarks', 'Remarks', 
            'Subject Heading', 'Physical_Status', 'System_Count', 'Physical_Count'
        ]
        display_cols = [col for col in display_cols if col in excel_view.columns]
        st.dataframe(excel_view[display_cols], use_container_width=True, hide_index=True)
    except Exception as e:
        st.info("System Masterlist is currently empty. Please process the inventory data first.")
    conn.close()

#  6. ADMIN 
elif menu == "System Admin":
    st.header(" System Administration")
    conn = get_db_connection()
    if st.button("Clear Audit Logs"):
        conn.execute("DELETE FROM audit_log")
        conn.commit()
        st.success("System logs cleared successfully.")
    st.dataframe(pd.read_sql("SELECT * FROM audit_log ORDER BY timestamp DESC", conn), use_container_width=True, hide_index=True)
    conn.close()