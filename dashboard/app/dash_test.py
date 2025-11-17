from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
import pandas as pd
import calendar
import time
import logging

from test_poll import poll_for_pipeline_run
from constants import get_md_connection

# ==============================================================================
# CONFIGURATION
# ==============================================================================
CACHE_TTL = 1800  
POLL_INTERVAL = "2s"
CHART_HEIGHT_TREND = 400
CHART_HEIGHT_DONUT = 400
CHART_HEIGHT_TREEMAP = 600
CHART_HEIGHT_BARS = 300
COLOR_SCHEME = px.colors.sequential.Viridis_r
LOOKBACK_DAYS = 180 

# ==============================================================================
# LOGGING SETUP
# ==============================================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)
log = logging.getLogger(__name__)

# ==============================================================================
# SESSION STATE INITIALIZATION
# ==============================================================================
if 'last_refresh_time' not in st.session_state:
    st.session_state.last_refresh_time = datetime.now(timezone.utc).isoformat()
    
if 'last_poll_check' not in st.session_state:
    st.session_state.last_poll_check = time.time()

# ==============================================================================
# DATABASE CONNECTION
# ==============================================================================
@st.cache_resource
def get_connection():
    """Get cached database connection to avoid recreating on every rerun."""
    try:
        return get_md_connection()
    except Exception as e:
        log.error(f"Failed to establish database connection: {e}")
        raise

_conn = get_connection()

# ==============================================================================
# DATA LOADING FUNCTIONS
# ==============================================================================
@st.cache_data(ttl=CACHE_TTL)
def load_available_budget():
    """Load available budget data with caching."""
    try:
        return _conn.execute("""
            SELECT available_budget, available_total
            FROM b_app.sem.available
        """).df()
    except Exception as e:
        log.error(f"Failed to load available budget: {e}")
        raise

@st.cache_data(ttl=CACHE_TTL)
def load_summary_data():
    """Load main spending summary data with caching."""
    try:
        return _conn.execute("""
            SELECT
                year_month,
                spending_category,
                spent_at,
                ROUND(SUM(AMOUNT)) as total_amount
            FROM b_app.sem.spending
            WHERE space = 'Default'
              AND spending_category != 'bills and services'
            GROUP BY year_month, spending_category, spent_at
            ORDER BY total_amount DESC
        """).df()
    except Exception as e:
        log.error(f"Failed to load summary data: {e}")
        raise

def get_filtered_spending(summary_df: pd.DataFrame, start_month: str, end_month: str) -> pd.DataFrame:
    """
    Filter spending data by date range using parameterized query.
    
    This prevents SQL injection and makes the code more maintainable.
    Even though we're using DuckDB on DataFrames (not raw user input),
    this pattern is important for consistency and when migrating to SQL Server.
    """
    try:
        query = """
            SELECT
                spending_category,
                spent_at,
                SUM(total_amount) as total_amount
            FROM summary_df 
            WHERE year_month >= ? AND year_month <= ?
            GROUP BY spending_category, spent_at
            ORDER BY total_amount DESC
        """
        return _conn.execute(query, [start_month, end_month]).df()
    except Exception as e:
        log.error(f"Failed to filter spending data: {e}")
        raise

def get_trend_data(summary_df: pd.DataFrame, lookback_days: int = LOOKBACK_DAYS) -> pd.DataFrame:
    """Get spending trend for the last N days."""
    try:
        cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m')
        query = """
            SELECT 
                year_month,
                SUM(total_amount) as monthly_total
            FROM summary_df
            WHERE year_month >= ?
            GROUP BY year_month
            ORDER BY year_month
        """
        return _conn.execute(query, [cutoff_date]).df()
    except Exception as e:
        log.error(f"Failed to get trend data: {e}")
        raise

def get_months_in_range(summary_df: pd.DataFrame, start_month: str, end_month: str) -> int:
    """Calculate number of months in the selected range."""
    try:
        query = """
            SELECT COUNT(DISTINCT year_month) as num_months
            FROM summary_df
            WHERE year_month >= ? AND year_month <= ?
        """
        return _conn.execute(query, [start_month, end_month]).df()['num_months'][0]
    except Exception as e:
        log.error(f"Failed to count months in range: {e}")
        return 1

# ==============================================================================
# CHART CREATION FUNCTIONS
# ==============================================================================
def create_trend_chart(trend_df: pd.DataFrame) -> go.Figure:
    """Create 6-month spending trend line chart."""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=trend_df['year_month'],
        y=trend_df['monthly_total'],
        mode='lines+markers',
        name='Actual Spending',
        line=dict(color='#440154', width=3),
        marker=dict(size=8),
        hovertemplate='<b>%{x}</b><br>£%{y:,.0f}<extra></extra>'
    ))
    
    fig.update_layout(
        title='6-Month Spending Trend',
        xaxis_title='Month',
        yaxis_title='Amount (£)',
        height=CHART_HEIGHT_TREND,
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

def create_donut_chart(category_totals: pd.DataFrame) -> go.Figure:
    """Create donut chart for spending by category."""
    total_spending_calc = category_totals['total_amount'].sum()
    
    fig = px.pie(
        category_totals,
        names='spending_category',
        values='total_amount',
        color_discrete_sequence=COLOR_SCHEME,
        hole=0.5
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>£%{value:,.0f}<extra></extra>'
    )
    
    fig.add_annotation(
        text=f'£{total_spending_calc:,.0f}',
        x=0.5, y=0.52,
        font_size=20,
        font_weight='bold',
        showarrow=False
    )
    
    fig.add_annotation(
        text='Total',
        x=0.5, y=0.45,
        font_size=12,
        font_color='gray',
        showarrow=False
    )
    
    fig.update_layout(
        height=CHART_HEIGHT_DONUT,
        showlegend=False,
        margin=dict(t=0, b=0, l=0, r=0)
    )
    
    return fig

def create_top_stores_chart(top_locations: pd.DataFrame) -> go.Figure:
    """Create horizontal bar chart for top 5 stores."""
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=top_locations['spent_at'],
        x=top_locations['total_amount'],
        orientation='h',
        marker=dict(
            color=top_locations['total_amount'],
            colorscale=COLOR_SCHEME,
            showscale=False
        ),
        text=[f"£{val:,.0f}" for val in top_locations['total_amount']],
        textposition='inside',
        hovertemplate='<b>%{y}</b><br>£%{x:,.0f}<extra></extra>'
    ))
    
    fig.update_layout(
        height=CHART_HEIGHT_BARS,
        margin=dict(t=0, b=0, l=0, r=0),
        xaxis_title=None,
        yaxis_title=None,
        showlegend=False,
        xaxis=dict(showticklabels=False, showgrid=False),
        yaxis=dict(autorange="reversed")
    )
    
    return fig

def create_treemap(filtered_df: pd.DataFrame) -> go.Figure:
    """Create treemap for detailed spending breakdown."""
    fig = px.treemap(
        filtered_df,
        path=['spending_category', 'spent_at'],
        values='total_amount',
        color='total_amount',
        color_continuous_scale=COLOR_SCHEME,
    )
    
    fig.update_traces(
        textinfo='label+value+percent parent',
        hovertemplate='<b>%{label}</b><br>Amount: £%{value:,.2f}<br>%{percentParent:.1%}<extra></extra>',
        texttemplate='<b>%{label}</b><br>£%{value:,.0f}<br>%{percentParent:.1%}'
    )
    
    fig.update_layout(
        height=CHART_HEIGHT_TREEMAP,
        coloraxis_showscale=False
    )
    
    return fig

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================
def format_month(month_str: str) -> str:
    """Convert YYYY-MM to 'MMM YYYY' format for display."""
    dt = datetime.strptime(month_str, '%Y-%m')
    return dt.strftime('%b %Y')

# ==============================================================================
# AUTO-REFRESH POLLING
# ==============================================================================
@st.fragment(run_every=POLL_INTERVAL)
def poll_and_refresh():
    """
    Poll for pipeline runs and refresh dashboard if needed.
    Uses timestamp-based approach to prevent rapid re-triggers.
    """
    try:
        current_time = time.time()
        
        # Prevent checks within 5 seconds of last refresh
        if current_time - st.session_state.last_poll_check < 5:
            return
        
        should_refresh, _ = poll_for_pipeline_run(
            pipeline_names=("main-pipeline", "webhook-pipeline"),
            last_refreshed_iso=st.session_state.last_refresh_time,
        )

        if should_refresh:
            log.info("Refreshing dashboard due to pipeline run.")
            
            # Update timestamps
            st.session_state.last_refresh_time = datetime.now(timezone.utc).isoformat()
            st.session_state.last_poll_check = current_time
            
            # Clear cache and refresh
            st.cache_data.clear()
            st.rerun()

    except Exception as e:
        log.error("Polling error : {e}")

# Start polling
poll_and_refresh()

# ==============================================================================
# MAIN APPLICATION
# ==============================================================================

# Load data with error handling
try:
    with st.spinner('Loading data...'):
        available_df = load_available_budget()
        summary_df = load_summary_data()
except Exception as e:
    st.error("Unable to load data from database. Please try refreshing the page.")
    st.exception(e)
    st.stop()

# Get unique months for filter
try:
    months_query = """
        SELECT DISTINCT year_month
        FROM summary_df
        ORDER BY year_month DESC
    """
    all_months = sorted(_conn.execute(months_query).df()['year_month'].unique())
except Exception as e:
    log.error(f"Failed to get months list: {e}")
    st.error("Unable to load date filter options.")
    st.stop()

# ==============================================================================
# SIDEBAR FILTERS
# ==============================================================================
st.sidebar.subheader("Date Range")

# Format months for display
formatted_months = [format_month(m) for m in all_months]
month_mapping = dict(zip(formatted_months, all_months))

start_month_formatted = st.sidebar.selectbox(
    "From:",
    options=formatted_months,
    index=len(formatted_months) - 1,
    key="start_month"
)
start_month = month_mapping[start_month_formatted]

end_month_formatted = st.sidebar.selectbox(
    "To:",
    options=formatted_months,
    index=len(formatted_months) - 1,
    key="end_month"
)
end_month = month_mapping[end_month_formatted]

# Validate date range
if start_month > end_month:
    st.sidebar.error("⚠️ Start month must be before or equal to end month")
    st.stop()

if st.sidebar.button("Refresh Data", width='stretch'):
    st.cache_data.clear()
    st.rerun()

# ==============================================================================
# DATA PROCESSING
# ==============================================================================
try:
    # Get filtered data
    filtered_df = get_filtered_spending(summary_df, start_month, end_month)
    
    # Get trend data (unfiltered, last 6 months)
    trend_df = get_trend_data(summary_df)
    
    # Calculate metrics
    total_spending = filtered_df['total_amount'].sum()
    available_budget = available_df['available_budget'].sum()
    
    now = datetime.now()
    days_in_month = calendar.monthrange(now.year, now.month)[1]
    available_budget_per_day = available_budget / days_in_month
    
    months_in_range = get_months_in_range(summary_df, start_month, end_month)
    
except Exception as e:
    log.error(f"Failed to process data: {e}")
    st.error("Unable to process spending data.")
    st.exception(e)
    st.stop()

# ==============================================================================
# METRICS DISPLAY
# ==============================================================================
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="Total Budget Remaining",
        value=f"£{available_budget:,.0f}"
    )

with col2:
    st.metric(
        label="Budget Remaining Per Day",
        value=f"£{available_budget_per_day:,.0f}"
    )

with col3:
    st.metric(
        label=f"Total Spent ({months_in_range} month{'s' if months_in_range != 1 else ''})",
        value=f"£{total_spending:,.0f}"
    )

st.divider()

# ==============================================================================
# TREND CHART
# ==============================================================================
st.title("Where My Money @ Analysis")

fig_trend = create_trend_chart(trend_df)
st.plotly_chart(fig_trend, width='stretch')

st.divider()

# ==============================================================================
# DETAILED BREAKDOWN
# ==============================================================================
st.subheader(f'Detailed Spending Breakdown ({start_month} to {end_month})')

col_left, col_right = st.columns([1, 2])

# Donut chart
with col_left:
    category_totals = filtered_df.groupby('spending_category', as_index=False)['total_amount'].sum()
    fig_donut = create_donut_chart(category_totals)
    st.plotly_chart(fig_donut, width='stretch')

# Top 5 stores
with col_right:
    top_locations = filtered_df.nlargest(5, 'total_amount')
    st.text("Top 5 Stores")
    fig_bars = create_top_stores_chart(top_locations)
    st.plotly_chart(fig_bars, width='stretch')

# Treemap
fig_tree = create_treemap(filtered_df)
st.plotly_chart(fig_tree, width='stretch')