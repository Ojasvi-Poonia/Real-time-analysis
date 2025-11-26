"""
Cassandra Real-Time Analytics Dashboard
Professional UI for demonstrating Apache Cassandra capabilities
"""

import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
import time
import altair as alt
from datetime import datetime
import os

# Page Configuration
st.set_page_config(
    page_title="Cassandra Analytics Dashboard",
    page_icon="C*",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Professional CSS - Clean, minimal design
st.markdown("""
<style>
    /* Main background - clean dark theme */
    .stApp {
        background-color: #0e1117;
    }
    
    /* Headers */
    h1 {
        color: #fafafa !important;
        font-weight: 600 !important;
        font-size: 1.75rem !important;
        letter-spacing: -0.02em;
        border-bottom: 1px solid #262730;
        padding-bottom: 0.75rem;
    }
    
    h2 {
        color: #fafafa !important;
        font-weight: 500 !important;
        font-size: 1.1rem !important;
        margin-top: 1.5rem !important;
    }
    
    h3 {
        color: #c2c2c2 !important;
        font-weight: 500 !important;
        font-size: 0.95rem !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    /* Metric styling */
    div[data-testid="stMetricValue"] {
        font-size: 1.5rem;
        font-weight: 600;
        color: #fafafa;
    }
    
    div[data-testid="stMetricLabel"] {
        color: #808495;
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    div[data-testid="stMetricDelta"] {
        font-size: 0.75rem;
    }
    
    /* Card style containers */
    .dashboard-card {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 6px;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    
    /* Status indicator */
    .status-indicator {
        display: inline-block;
        width: 8px;
        height: 8px;
        background-color: #238636;
        border-radius: 50%;
        margin-right: 6px;
    }
    
    .status-text {
        color: #808495;
        font-size: 0.8rem;
    }
    
    /* Latest transaction card */
    .transaction-card {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-left: 3px solid #238636;
        border-radius: 6px;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    
    .transaction-amount {
        font-size: 1.5rem;
        font-weight: 600;
        color: #fafafa;
    }
    
    .transaction-details {
        color: #808495;
        font-size: 0.85rem;
        margin-top: 0.5rem;
    }
    
    .transaction-meta {
        color: #6e7681;
        font-size: 0.75rem;
        margin-top: 0.5rem;
    }
    
    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background-color: #0d1117;
        border-right: 1px solid #21262d;
    }
    
    section[data-testid="stSidebar"] h1 {
        font-size: 1.1rem !important;
        border-bottom: none;
    }
    
    /* Code blocks */
    .cql-query {
        background-color: #0d1117;
        border: 1px solid #30363d;
        border-radius: 4px;
        padding: 0.5rem 0.75rem;
        font-family: 'SF Mono', 'Consolas', monospace;
        font-size: 0.75rem;
        color: #7ee787;
        margin: 0.5rem 0;
    }
    
    /* Table header */
    .table-header {
        color: #808495;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid #21262d;
        margin-bottom: 0.75rem;
    }
    
    /* Info box */
    .info-box {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 4px;
        padding: 0.75rem;
        margin: 0.5rem 0;
        font-size: 0.8rem;
        color: #c9d1d9;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Dataframe styling */
    .stDataFrame {
        border: 1px solid #30363d;
        border-radius: 6px;
    }
</style>
""", unsafe_allow_html=True)

# Helper Functions
def format_category(category):
    """Format category names for display"""
    if not category:
        return ""
    return category.replace("_", " ").title()

def clean_merchant(merchant):
    """Remove fraud_ prefix from merchant names"""
    if isinstance(merchant, str) and merchant.startswith("fraud_"):
        return merchant[6:]
    return merchant

# Cassandra Connection
@st.cache_resource
def get_session():
    hosts = [os.environ.get('CASSANDRA_HOST', 'cassandra'), '127.0.0.1', 'localhost']
    
    for host in hosts:
        try:
            cluster = Cluster([host], port=9042)
            session = cluster.connect()
            session.set_keyspace('payment_analytics')
            return session, cluster, host
        except:
            continue
    return None, None, None

session, cluster, connected_host = get_session()

# Sidebar
with st.sidebar:
    st.markdown("# Cassandra Demo")
    
    if session:
        st.markdown(
            '<span class="status-indicator"></span><span class="status-text">Connected</span>',
            unsafe_allow_html=True
        )
        st.caption(f"Host: {connected_host}")
    else:
        st.error("Disconnected")
    
    st.markdown("---")
    st.markdown("### Concepts Demonstrated")
    
    concepts = [
        "Query-First Modeling",
        "Partition Keys",
        "Clustering Columns", 
        "Counter Tables",
        "Denormalization",
        "Time-Series Pattern",
        "TTL (Auto-Expiry)"
    ]
    
    for concept in concepts:
        st.markdown(f"- {concept}")
    
    st.markdown("---")
    st.markdown("### Tables in Use")
    
    tables = [
        ("transactions_by_user", "Primary transaction log"),
        ("transactions_by_category", "Category-based queries"),
        ("spending_by_category", "Aggregated totals"),
        ("hourly_transactions", "Time-series data"),
        ("merchant_statistics", "Merchant analytics"),
        ("payment_method_stats", "Payment breakdown"),
    ]
    
    for table, desc in tables:
        st.markdown(f"**{table}**")
        st.caption(desc)

# Main Content
if session is None:
    st.error("Cannot connect to Cassandra. Ensure the database is running.")
    st.code("docker-compose up", language="bash")
    st.stop()

# Session state
if 'last_txn_id' not in st.session_state:
    st.session_state.last_txn_id = None
if 'txn_count' not in st.session_state:
    st.session_state.txn_count = 0

# Header
st.markdown("# Cassandra Real-Time Analytics")
st.caption("Demonstrating distributed database concepts through live payment processing")

# Connection status bar
col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    st.markdown(
        '<span class="status-indicator"></span><span class="status-text">Live</span>',
        unsafe_allow_html=True
    )
with col2:
    st.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")

st.markdown("---")

# Metrics Row
st.markdown("## Live Metrics")
metric_cols = st.columns(5)

total_txn_metric = metric_cols[0].empty()
total_amount_metric = metric_cols[1].empty()
avg_txn_metric = metric_cols[2].empty()
categories_metric = metric_cols[3].empty()
merchants_metric = metric_cols[4].empty()

# Main Dashboard Grid
st.markdown("---")
left_col, right_col = st.columns([3, 2])

with left_col:
    st.markdown("## Transaction Feed")
    st.markdown(
        '<div class="cql-query">SELECT * FROM transactions_by_user WHERE user_id=\'User_1\' LIMIT 20</div>',
        unsafe_allow_html=True
    )
    
    latest_txn_box = st.empty()
    feed_placeholder = st.empty()

with right_col:
    st.markdown("## Spending by Category")
    st.markdown(
        '<div class="cql-query">SELECT * FROM spending_by_category</div>',
        unsafe_allow_html=True
    )
    category_chart = st.empty()
    
    st.markdown("## Payment Methods")
    payment_chart = st.empty()

# Bottom Section
st.markdown("---")
bottom_left, bottom_right = st.columns(2)

with bottom_left:
    st.markdown("## Top Merchants")
    st.markdown(
        '<div class="cql-query">SELECT * FROM merchant_statistics</div>',
        unsafe_allow_html=True
    )
    merchant_chart = st.empty()

with bottom_right:
    st.markdown("## Hourly Volume")
    st.markdown(
        '<div class="cql-query">SELECT * FROM hourly_transactions WHERE hour_bucket = ?</div>',
        unsafe_allow_html=True
    )
    hourly_chart = st.empty()

# CQL Reference Section
st.markdown("---")
st.markdown("## Query Reference")

ref_col1, ref_col2 = st.columns(2)

with ref_col1:
    st.markdown("### Read Operations")
    st.code("""-- Fetch recent transactions (requires partition key)
SELECT * FROM transactions_by_user 
WHERE user_id = 'User_1' 
LIMIT 20;

-- Query by category (compound partition key)
SELECT * FROM transactions_by_category 
WHERE user_id = 'User_1' 
  AND category = 'grocery_pos';""", language="sql")

with ref_col2:
    st.markdown("### Counter Operations")
    st.code("""-- Atomic increment (no read required)
UPDATE spending_by_category 
SET total_amount = total_amount + 5000,
    transaction_count = transaction_count + 1 
WHERE category = 'grocery_pos';

-- Read aggregated totals
SELECT * FROM spending_by_category;""", language="sql")

# Color scheme for charts (professional, muted palette)
color_scheme = ['#539bf5', '#57ab5a', '#c69026', '#e5534b', '#986ee2', '#768390', '#39c5cf', '#d4a72c']

# Refresh Loop
while True:
    try:
        # Query 1: Recent Transactions
        rows = session.execute(
            "SELECT * FROM transactions_by_user WHERE user_id='User_1' LIMIT 30"
        )
        df = pd.DataFrame(list(rows))
        
        if not df.empty:
            df['merchant_clean'] = df['merchant'].apply(clean_merchant)
            df['category_display'] = df['category'].apply(format_category)
            
            # Metrics
            current_count = len(df)
            total_spent = float(df['amount'].sum())
            avg_amount = float(df['amount'].mean())
            
            # Track new transactions
            latest_id = str(df['transaction_id'].iloc[0])
            if st.session_state.last_txn_id != latest_id:
                st.session_state.txn_count += 1
                st.session_state.last_txn_id = latest_id
            
            with total_txn_metric:
                st.metric("Transactions", current_count, f"+{st.session_state.txn_count}")
            
            with total_amount_metric:
                st.metric("Total Volume", f"${total_spent:,.2f}")
            
            with avg_txn_metric:
                st.metric("Avg Transaction", f"${avg_amount:.2f}")
            
            # Latest transaction display
            latest = df.iloc[0]
            with latest_txn_box:
                st.markdown(f"""
                <div class="transaction-card">
                    <div class="transaction-amount">${float(latest['amount']):,.2f}</div>
                    <div class="transaction-details">
                        {clean_merchant(latest['merchant'])} | {format_category(latest['category'])}
                    </div>
                    <div class="transaction-meta">
                        {pd.to_datetime(latest['transaction_time']).strftime('%H:%M:%S')} | {latest['payment_method']}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            # Transaction table
            display_df = df.head(12)[['transaction_time', 'merchant_clean', 'category_display', 'amount', 'payment_method']].copy()
            display_df['transaction_time'] = pd.to_datetime(display_df['transaction_time']).dt.strftime('%H:%M:%S')
            display_df['amount'] = display_df['amount'].apply(lambda x: f"${float(x):,.2f}")
            display_df.columns = ['Time', 'Merchant', 'Category', 'Amount', 'Method']
            
            with feed_placeholder:
                st.dataframe(display_df, hide_index=True, use_container_width=True, height=300)
        
        # Query 2: Category Spending
        cat_rows = session.execute("SELECT * FROM spending_by_category")
        df_cat = pd.DataFrame(list(cat_rows))
        
        if not df_cat.empty:
            df_cat['total_dollars'] = df_cat['total_amount'] / 100
            df_cat['category_display'] = df_cat['category'].apply(format_category)
            
            with categories_metric:
                st.metric("Categories", len(df_cat))
            
            cat_chart = alt.Chart(df_cat).mark_arc(innerRadius=40, outerRadius=80).encode(
                theta=alt.Theta("total_dollars:Q"),
                color=alt.Color(
                    "category_display:N",
                    scale=alt.Scale(range=color_scheme),
                    legend=alt.Legend(title=None, orient="right", labelFontSize=10)
                ),
                tooltip=[
                    alt.Tooltip("category_display:N", title="Category"),
                    alt.Tooltip("total_dollars:Q", title="Total", format="$,.0f")
                ]
            ).properties(height=200)
            
            with category_chart:
                st.altair_chart(cat_chart, use_container_width=True)
        
        # Query 3: Payment Methods
        pay_rows = session.execute("SELECT * FROM payment_method_stats")
        df_pay = pd.DataFrame(list(pay_rows))
        
        if not df_pay.empty:
            pay_chart = alt.Chart(df_pay).mark_bar(cornerRadiusEnd=3).encode(
                x=alt.X("transaction_count:Q", title=None, axis=alt.Axis(labels=False, ticks=False)),
                y=alt.Y("payment_method:N", sort='-x', title=None),
                color=alt.Color(
                    "payment_method:N",
                    scale=alt.Scale(range=color_scheme),
                    legend=None
                ),
                tooltip=["payment_method:N", "transaction_count:Q"]
            ).properties(height=120)
            
            with payment_chart:
                st.altair_chart(pay_chart, use_container_width=True)
        
        # Query 4: Merchant Stats
        merch_rows = session.execute("SELECT * FROM merchant_statistics")
        df_merch = pd.DataFrame(list(merch_rows))
        
        if not df_merch.empty:
            df_merch['total_dollars'] = df_merch['total_amount'] / 100
            df_merch['merchant_clean'] = df_merch['merchant'].apply(clean_merchant)
            df_merch = df_merch.nlargest(8, 'total_dollars')
            
            with merchants_metric:
                st.metric("Merchants", len(df_merch))
            
            merch_chart = alt.Chart(df_merch).mark_bar(cornerRadiusEnd=3).encode(
                x=alt.X("total_dollars:Q", title="Volume ($)"),
                y=alt.Y("merchant_clean:N", sort='-x', title=None),
                color=alt.value('#539bf5'),
                tooltip=[
                    alt.Tooltip("merchant_clean:N", title="Merchant"),
                    alt.Tooltip("total_dollars:Q", title="Total", format="$,.2f"),
                    alt.Tooltip("transaction_count:Q", title="Count")
                ]
            ).properties(height=250)
            
            with merchant_chart:
                st.altair_chart(merch_chart, use_container_width=True)
        
        # Query 5: Hourly Trend
        current_hour = datetime.now().strftime('%Y-%m-%d-%H')
        hourly_rows = session.execute(
            "SELECT * FROM hourly_transactions WHERE hour_bucket = %s LIMIT 100",
            (current_hour,)
        )
        df_hourly = pd.DataFrame(list(hourly_rows))
        
        if not df_hourly.empty:
            df_hourly['minute'] = pd.to_datetime(df_hourly['transaction_time']).dt.minute
            minute_agg = df_hourly.groupby('minute').agg({
                'amount': ['sum', 'count']
            }).reset_index()
            minute_agg.columns = ['minute', 'total', 'count']
            
            hourly_line = alt.Chart(minute_agg).mark_area(
                line={'color': '#539bf5'},
                color=alt.Gradient(
                    gradient='linear',
                    stops=[
                        alt.GradientStop(color='rgba(83, 155, 245, 0.3)', offset=0),
                        alt.GradientStop(color='rgba(83, 155, 245, 0.05)', offset=1)
                    ],
                    x1=1, x2=1, y1=1, y2=0
                )
            ).encode(
                x=alt.X('minute:Q', title='Minute', axis=alt.Axis(tickCount=5)),
                y=alt.Y('count:Q', title='Transactions'),
                tooltip=['minute:Q', 'count:Q', alt.Tooltip('total:Q', format='$,.2f')]
            ).properties(height=250)
            
            with hourly_chart:
                st.altair_chart(hourly_line, use_container_width=True)
        else:
            with hourly_chart:
                st.caption("Collecting data...")
    
    except Exception as e:
        st.error(f"Error: {e}")
    
    time.sleep(1)
    st.rerun()
