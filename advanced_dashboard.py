# dashboard.py (FINAL PRO VERSION)
import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- Page Config ---
st.set_page_config(
    page_title="Traffic Violation Analysis Dashboard",
    page_icon="🚨",
    layout="wide"
)

# --- Environment Setup ---
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

# --- Data Loading ---
@st.cache_data
def load_main_data():
    """
    Loads the single, clean dataset from the pipeline.
    Caching ensures this expensive I/O operation runs only once.
    """
    try:
        df = pd.read_parquet("cleaned_traffic_data.parquet")
    except FileNotFoundError:
        st.error("Error: `cleaned_traffic_data.parquet` not found.")
        st.error("Please run the `clean_data.py` script first.")
        return None
    
    # --- Feature Engineering (on load) ---
    df['hour'] = df['Timestamp'].dt.hour
    df['dayofweek'] = df['Timestamp'].dt.dayofweek  # Pandas: Mon=0, Sun=6
    df['day_name'] = df['Timestamp'].dt.day_name()
    df['day_type'] = df['dayofweek'].apply(lambda x: 'Weekend' if x >= 5 else 'Weekday') # 5=Sat, 6=Sun
    df['date_only'] = df['Timestamp'].dt.date
    return df

# Load the data
df_main = load_main_data()

if df_main is None:
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header("Dashboard Filters")

all_locations = df_main['Location'].unique()
selected_locations = st.sidebar.multiselect(
    "Select Location(s):",
    all_locations,
    default=all_locations
)

all_types = df_main['Violation_Type'].unique()
selected_types = st.sidebar.multiselect(
    "Select Violation Type(s):",
    all_types,
    default=all_types
)

min_date = df_main['date_only'].min()
max_date = df_main['date_only'].max()

selected_date_range = st.sidebar.date_input(
    "Select Date Range:",
    [min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

# --- Dynamic Filtering Logic ---
if len(selected_date_range) == 2:
    start_date, end_date = selected_date_range
    mask = (
        (df_main['Location'].isin(selected_locations)) &
        (df_main['Violation_Type'].isin(selected_types)) &
        (df_main['date_only'] >= start_date) &
        (df_main['date_only'] <= end_date)
    )
    df_filtered = df_main[mask]
else:
    df_filtered = pd.DataFrame() # Create an empty DataFrame if no valid date

# --- Dashboard Title ---
st.title("🚨 Smart Traffic Violation Analysis")
st.markdown("This dynamic dashboard analyzes patterns from the PySpark data pipeline.")

# --- Key Performance Indicators (KPIs) ---
st.markdown("### Dynamic KPIs (Key Performance Indicators)")

# Safely calculate KPIs, with a fallback for empty data
if not df_filtered.empty:
    total_violations = df_filtered.shape[0]
    top_violation_type = df_filtered['Violation_Type'].mode()[0]
    busiest_day = df_filtered['day_name'].mode()[0]
else:
    total_violations = 0
    top_violation_type = "N/A"
    busiest_day = "N/A"

kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Total Violations", f"{total_violations:,}")
kpi2.metric("Top Violation Type", top_violation_type)
kpi3.metric("Busiest Day", busiest_day)

st.markdown("---")

# --- Tabs for Organized Layout ---
tab1, tab2, tab3 = st.tabs(["🕒 Time Analysis", "📍 Location & Type Analysis", "💾 Raw Data Export"])

# --- THIS IS THE FIX ---
# We add `if not df_filtered.empty:` to every chart block.
# We also add an `else:` block to show a user-friendly message.

with tab1:
    st.header("🕒 Time-Based Violation Analysis")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Violations by Hour of Day")
        if not df_filtered.empty:
            hourly_counts = df_filtered['hour'].value_counts().sort_index()
            fig_hour = px.bar(
                hourly_counts, 
                x=hourly_counts.index, 
                y=hourly_counts.values,
                labels={'x': 'Hour of Day', 'y': 'Total Violations'}
            )
            st.plotly_chart(fig_hour, use_container_width=True)
        else:
            st.info("No data to display for the selected filters.")

    with col2:
        st.markdown("#### Violations by Day of Week")
        if not df_filtered.empty:
            daily_counts = df_filtered['day_name'].value_counts().reindex(
                ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            )
            fig_day = px.bar(
                daily_counts, 
                x=daily_counts.index, 
                y=daily_counts.values,
                labels={'x': 'Day of Week', 'y': 'Total Violations'}
            )
            st.plotly_chart(fig_day, use_container_width=True)
        else:
            st.info("No data to display for the selected filters.")

    st.markdown("#### Heatmap: Violation Hotspots (Hour vs. Day)")
    if not df_filtered.empty:
        pivot_table = pd.crosstab(df_filtered['Violation_Type'], df_filtered['hour'])
        fig_heatmap = px.imshow(
            pivot_table,
            labels=dict(x="Hour of Day", y="Violation Type", color="Count"),
            x=pivot_table.columns,
            y=pivot_table.index,
            text_auto=True,
            aspect="auto",
            color_continuous_scale="Reds"
        )
        fig_heatmap.update_layout(title_text="Violation Count by Type and Hour")
        st.plotly_chart(fig_heatmap, use_container_width=True)
    else:
        st.info("No data to display for the selected filters.")


with tab2:
    st.header("📍 Location & Type Analysis")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("#### Top 10 High-Violation Locations")
        if not df_filtered.empty:
            top_10_locations = df_filtered['Location'].value_counts().head(10)
            fig_loc = px.bar(
                top_10_locations, 
                y=top_10_locations.index, 
                x=top_10_locations.values,
                orientation='h',
                labels={'x': 'Total Violations', 'y': 'Location'},
                text=top_10_locations.values
            )
            fig_loc.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_loc, use_container_width=True)
        else:
            st.info("No data to display for the selected filters.")

    with col2:
        st.markdown("#### Violation Type Distribution")
        if not df_filtered.empty:
            type_dist = df_filtered['Violation_Type'].value_counts()
            fig_donut = px.pie(
                type_dist, 
                values=type_dist.values, 
                names=type_dist.index,
                hole=0.4
            )
            fig_donut.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            st.info("No data to display for the selected filters.")

with tab3:
    st.header("💾 Raw Data Export")
    st.markdown("Download the currently filtered data as a CSV report.")
    
    @st.cache_data
    def convert_df_to_csv(df):
        return df.to_csv(index=False).encode('utf-8')

    csv_data = convert_df_to_csv(df_filtered)

    st.download_button(
        label="Download Filtered Data as CSV",
        data=csv_data,
        file_name="filtered_traffic_report.csv",
        mime="text/csv",
    )
    
    st.markdown("### Filtered Data Preview")
    st.dataframe(df_filtered.head(100))