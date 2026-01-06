import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
import time

# ---------------- Pipeline Class ----------------
class Pipeline:
    """Daily Agent KPI Pipeline"""
    def __init__(self, file):
        self.file = file
        self.cho = None
        self.cht = None
        self.chd = None
        self.che = None
        self.base = None
        self.stats = {}

    @staticmethod
    def standardize(df):
        df.columns = [c.lower().strip() for c in df.columns]
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        if 'agentmis' in df.columns:
            df['agentmis'] = df['agentmis'].astype(str)
        return df

    @staticmethod
    def count_fail(series):
        return (series.astype(str).str.lower() == 'fail').sum()

    def validate_sheets(self):
        """Validate required sheets exist"""
        required_sheets = ["CHO", "CHT", "CHD", "CHE"]
        available_sheets = pd.ExcelFile(self.file).sheet_names
        missing = [s for s in required_sheets if s not in available_sheets]
        if missing:
            raise ValueError(f"Missing required sheets: {', '.join(missing)}")
        return True

    def load_sheets(self):
        self.validate_sheets()
        self.cho = self.standardize(pd.read_excel(self.file, sheet_name="CHO"))
        self.cht = self.standardize(pd.read_excel(self.file, sheet_name="CHT"))
        self.chd = self.standardize(pd.read_excel(self.file, sheet_name="CHD"))
        self.che = self.standardize(pd.read_excel(self.file, sheet_name="CHE"))
        
        # Collect stats
        self.stats['cho_rows'] = len(self.cho)
        self.stats['cht_rows'] = len(self.cht)
        self.stats['chd_rows'] = len(self.chd)
        self.stats['che_rows'] = len(self.che)

    def build_base(self):
        self.base = pd.concat([
            self.cho[['date','agentmis']],
            self.cht[['date','agentmis']],
            self.chd[['date','agentmis']],
            self.che[['date','agentmis']]
        ]).drop_duplicates().reset_index(drop=True)
        
        self.stats['unique_agents'] = self.base['agentmis'].nunique()
        self.stats['date_range'] = f"{self.base['date'].min().strftime('%Y-%m-%d')} to {self.base['date'].max().strftime('%Y-%m-%d')}"
        self.stats['total_records'] = len(self.base)

    def aggregate_cho(self):
        df = self.cho.copy()
        agg = df.groupby(['date','agentmis']).agg(
            absent=('status', lambda x: (x.astype(str).str.lower()=='no show').sum()),
            scheduled=('status', lambda x: (~x.astype(str).str.lower().isin(['off'])).sum())
        ).reset_index()
        self.base = self.base.merge(agg, on=['date','agentmis'], how='left')

    def aggregate_cht(self):
        df = self.cht.copy()
        agg = df.groupby(['date','agentmis']).agg(
            ans_vol=('ans_vol','sum'),
            aht_min=('aht','sum'),
            art_min=('art','sum')
        ).reset_index()
        self.base = self.base.merge(agg, on=['date','agentmis'], how='left')

    def aggregate_chd(self):
        df = self.chd.copy()
        def csat(x):
            if pd.api.types.is_numeric_dtype(x):
                return x[x.isin([4,5])].count()
            return 0
        def dsat(x):
            if pd.api.types.is_numeric_dtype(x):
                return x[x == 1].count()
            return 0
        agg = df.groupby(['date','agentmis']).agg(
            surveyed_count=('score','count'),
            solved_count=('sloved', lambda x: (x.astype(str).str.lower()=='yes').sum()),
            not_solved_count=('sloved', lambda x: (x.astype(str).str.lower()!='yes').sum()),
            csat_count=('score', csat),
            dsat_count=('score', dsat)
        ).reset_index()
        self.base = self.base.merge(agg, on=['date','agentmis'], how='left')

    def aggregate_che(self):
        df = self.che.copy()
        agg = df.groupby(['date','agentmis']).agg(
            evaluated_count=('final','count'),
            pass_eval_count=('final', lambda x: (x.astype(str).str.lower()=='pass').sum()),
            fail_eval_count=('final', lambda x: (x.astype(str).str.lower()=='fail').sum()),
            rc1_fail=('rc1', self.count_fail),
            rc2_fail=('rc2', self.count_fail),
            rc_fail=('rc', self.count_fail),
            bc_fail=('bc', self.count_fail),
            cc_fail=('cc', self.count_fail)
        ).reset_index()
        self.base = self.base.merge(agg, on=['date','agentmis'], how='left')

    def build_db(self):
        self.load_sheets()
        self.build_base()
        self.aggregate_cho()
        self.aggregate_cht()
        self.aggregate_chd()
        self.aggregate_che()
        metric_cols = [c for c in self.base.columns if c not in ['date','agentmis']]
        self.base[metric_cols] = self.base[metric_cols].fillna(0)
        return self.base

# ---------------- Streamlit App ----------------
st.set_page_config(page_title="Daily Agent KPI Generator", page_icon="📊", layout="wide")

# ---------------- Enhanced CSS ----------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

#MainMenu, footer, header {visibility: hidden;}

* {
    font-family: 'Inter', sans-serif;
}

.block-container {
    background: linear-gradient(135deg, #FFF5E6 0%, #FFE4D6 100%);
    border-radius: 16px;
    padding: 2.5rem;
    box-shadow: 0 4px 6px rgba(0,0,0,0.05);
}

/* Headers */
h1 {
    color: #3D2C5C;
    font-weight: 700;
    font-size: 2.5rem;
    margin-bottom: 0.5rem;
    text-shadow: 1px 1px 2px rgba(0,0,0,0.05);
}

h2, h3 {
    color: #574964;
    font-weight: 600;
}

.subtitle {
    color: #574964;
    font-size: 1.1rem;
    margin-bottom: 2rem;
}

/* Fix text colors */
p, span, div, label {
    color: #2C2C2C !important;
}

.stMarkdown p {
    color: #3D2C5C !important;
}

/* File uploader */
[data-testid="stFileUploader"] {
    background: white;
    border: 2px dashed #9F8383;
    border-radius: 12px;
    padding: 2rem;
    transition: all 0.3s ease;
}

[data-testid="stFileUploader"] label {
    color: #574964 !important;
}

[data-testid="stFileUploader"] p, [data-testid="stFileUploader"] span {
    color: #574964 !important;
}

[data-testid="stFileUploader"]:hover {
    border-color: #574964;
    box-shadow: 0 4px 12px rgba(87, 73, 100, 0.1);
}

/* Buttons */
.stButton>button {
    background: linear-gradient(135deg, #574964 0%, #6B5A7A 100%);
    color: white;
    font-weight: 600;
    border: none;
    border-radius: 8px;
    padding: 0.75rem 2rem;
    font-size: 1rem;
    box-shadow: 0 4px 12px rgba(87, 73, 100, 0.3);
    transition: all 0.3s ease;
}

.stButton>button:hover {
    background: linear-gradient(135deg, #6B5A7A 0%, #7B6B8E 100%);
    box-shadow: 0 6px 16px rgba(87, 73, 100, 0.4);
    transform: translateY(-2px);
}

.stDownloadButton>button {
    background: linear-gradient(135deg, #8B7E9E 0%, #9F8383 100%);
    color: white;
    font-weight: 600;
    border: none;
    border-radius: 8px;
    padding: 0.75rem 2rem;
    font-size: 1rem;
    box-shadow: 0 4px 12px rgba(159, 131, 131, 0.3);
}

.stDownloadButton>button:hover {
    background: linear-gradient(135deg, #9F8383 0%, #B39393 100%);
    transform: translateY(-2px);
}

/* Info/Success/Error boxes */
.stAlert {
    border-radius: 10px;
    border-left: 4px solid;
    padding: 1rem;
    margin: 1rem 0;
}

/* Stats cards */
.stat-card {
    background: white;
    border-radius: 12px;
    padding: 1.5rem;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    border-left: 4px solid #574964;
    margin: 0.5rem 0;
}

.stat-title {
    color: #7B6B8E;
    font-size: 0.9rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.stat-value {
    color: #3D2C5C;
    font-size: 1.8rem;
    font-weight: 700;
    margin-top: 0.5rem;
}

/* Dataframe */
div[data-testid="stDataFrame"] {
    background: white;
    border-radius: 12px;
    padding: 1rem;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
}

/* Progress bar */
.stProgress > div > div > div > div {
    background: linear-gradient(90deg, #574964 0%, #9F8383 100%);
}

/* Expander */
.streamlit-expanderHeader {
    background: white;
    border-radius: 8px;
    font-weight: 600;
    color: #574964 !important;
}

.streamlit-expanderHeader p {
    color: #574964 !important;
}

.streamlit-expanderContent {
    background: white;
    border-radius: 8px;
    padding: 1rem;
}

.streamlit-expanderContent p, .streamlit-expanderContent li {
    color: #3D2C5C !important;
}

/* Info/Alert boxes text */
.stAlert p {
    color: #2C2C2C !important;
}

/* Caption text */
.stCaption {
    color: #7B6B8E !important;
}

</style>
""", unsafe_allow_html=True)

# ---------------- Header ----------------
st.markdown("# 📊 Daily Agent KPI Generator")
st.markdown('<p class="subtitle">Upload your Excel workbook and generate comprehensive daily agent KPIs with detailed metrics</p>', unsafe_allow_html=True)

# ---------------- Instructions Expander ----------------
with st.expander("📖 How to use this tool"):
    st.markdown("""
    **Required Excel Structure:**
    - Your workbook must contain these sheets: **CHO**, **CHT**, **CHD**, **CHE**
    - Each sheet should have a `date` and `agentmis` column
    
    **What this tool does:**
    - Aggregates attendance, call handling, satisfaction, and evaluation metrics
    - Generates a unified daily agent database
    - Provides detailed statistics about your data
    
    **Steps:**
    1. Upload your Excel file below
    2. Review the processing statistics
    3. Preview the generated database
    4. Download the final output
    """)

# ---------------- File Upload ----------------
st.markdown("### 📁 Upload Your Data")
uploaded_file = st.file_uploader(
    "Choose an Excel workbook (.xlsx)", 
    type=["xlsx"],
    help="Upload a workbook containing CHO, CHT, CHD, and CHE sheets"
)

if uploaded_file:
    try:
        # Progress indicator
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text("🔍 Validating workbook structure...")
        progress_bar.progress(20)
        time.sleep(0.3)
        
        pipeline = Pipeline(uploaded_file)
        
        status_text.text("📥 Loading data sheets...")
        progress_bar.progress(40)
        time.sleep(0.3)
        
        status_text.text("🔄 Processing metrics...")
        progress_bar.progress(60)
        time.sleep(0.3)
        
        status_text.text("🧮 Aggregating KPIs...")
        progress_bar.progress(80)
        daily_db = pipeline.build_db()
        
        status_text.text("✅ Finalizing database...")
        progress_bar.progress(100)
        time.sleep(0.2)
        
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        
        st.success("✅ Daily Agent DB generated successfully!")
        
        # ---------------- Statistics Dashboard ----------------
        st.markdown("### 📈 Processing Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-title">Total Records</div>
                <div class="stat-value">{pipeline.stats['total_records']:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-title">Unique Agents</div>
                <div class="stat-value">{pipeline.stats['unique_agents']:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-title">Date Range</div>
                <div class="stat-value" style="font-size: 1.1rem;">{pipeline.stats['date_range']}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            total_rows = sum([pipeline.stats['cho_rows'], pipeline.stats['cht_rows'], 
                            pipeline.stats['chd_rows'], pipeline.stats['che_rows']])
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-title">Source Rows</div>
                <div class="stat-value">{total_rows:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Sheet details
        with st.expander("📋 Detailed Sheet Statistics"):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("CHO (Attendance)", f"{pipeline.stats['cho_rows']:,} rows")
            with col2:
                st.metric("CHT (Call Handling)", f"{pipeline.stats['cht_rows']:,} rows")
            with col3:
                st.metric("CHD (Satisfaction)", f"{pipeline.stats['chd_rows']:,} rows")
            with col4:
                st.metric("CHE (Evaluation)", f"{pipeline.stats['che_rows']:,} rows")
        
        # ---------------- Data Dictionary ----------------
        with st.expander("📚 Metrics Dictionary"):
            st.markdown("""
            **Attendance Metrics (CHO):**
            - `absent`: Number of no-show occurrences
            - `scheduled`: Number of scheduled shifts (excluding off days)
            
            **Call Handling Metrics (CHT):**
            - `ans_vol`: Total answered call volume
            - `aht_min`: Average Handle Time in minutes
            - `art_min`: Average Response Time in minutes
            
            **Satisfaction Metrics (CHD):**
            - `surveyed_count`: Total number of surveys received
            - `solved_count`: Number of issues marked as solved
            - `not_solved_count`: Number of unresolved issues
            - `csat_count`: Customer Satisfaction count (scores 4-5)
            - `dsat_count`: Dissatisfaction count (score 1)
            
            **Evaluation Metrics (CHE):**
            - `evaluated_count`: Total evaluations conducted
            - `pass_eval_count`: Number of passed evaluations
            - `fail_eval_count`: Number of failed evaluations
            - `rc1_fail`, `rc2_fail`, `rc_fail`: Regulatory compliance failures
            - `bc_fail`: Business compliance failures
            - `cc_fail`: Customer compliance failures
            """)
        
        # ---------------- Preview ----------------
        st.markdown("### 👀 Data Preview")
        st.caption(f"Showing first 20 of {len(daily_db):,} records")
        st.dataframe(daily_db.head(20), use_container_width=True)
        
        # ---------------- Download ----------------
        st.markdown("### 💾 Download Results")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            output = BytesIO()
            daily_db.to_excel(output, index=False, engine='openpyxl')
            output.seek(0)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"daily_agent_db_{timestamp}.xlsx"
            
            st.download_button(
                label="📥 Download Daily Agent Database",
                data=output,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col2:
            st.info(f"**File size:** {len(output.getvalue()) / 1024:.1f} KB")

    except ValueError as ve:
        st.error(f"❌ Validation Error: {ve}")
        st.info("💡 Please ensure your workbook contains all required sheets: CHO, CHT, CHD, CHE")
    
    except Exception as e:
        st.error(f"❌ Error processing workbook: {e}")
        st.info("💡 Please check that your Excel file structure matches the requirements in the 'How to use' section above")

else:
    # ---------------- Empty State ----------------
    st.info("👆 Upload an Excel workbook to get started")
    
    # Feature highlights
    st.markdown("### ✨ Features")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **📊 Comprehensive Metrics**
        
        Aggregates attendance, calls, satisfaction, and evaluation data into one unified database
        """)
    
    with col2:
        st.markdown("""
        **⚡ Fast Processing**
        
        Efficiently handles large datasets with progress tracking and validation
        """)
    
    with col3:
        st.markdown("""
        **📈 Detailed Stats**
        
        Get insights about your data with comprehensive statistics and summaries
        """)
