import streamlit as st
import pandas as pd
import plotly.express as px
import db_store
import stream_simulator
# Note: 'spark_processing' and 'requests' are no longer needed.

st.set_page_config(layout="wide", page_title="🦠 Epidemic Dashboard")

# -------------------------
# 1️⃣ Load and preprocess data (ONLY ONCE)
# -------------------------
@st.cache_data # Caches the data to make the app fast after the first load
def load_data(path):
    """
    This function now uses pandas to load the CSV, which is much faster
    for a single file than starting Spark.
    """
    # Using pandas for faster loading
    processed_data = pd.read_csv(path)
    
    processed_data['date'] = pd.to_datetime(processed_data['date'])
    db_store.register_data(processed_data) # Register data for the SQL queries
    return processed_data

# --- Main App ---
st.title("🦠 Real-Time Epidemic Spread Monitoring Dashboard")

# Load the data by calling the cached function
data_path = r"D:/sushmitha/Big_data_Project/data/epidemic_data.csv"
data = load_data(data_path)

# -------------------------
# 2️⃣ Sidebar filters
# -------------------------
st.sidebar.header("Filters")

# Default lists for regions and diseases
all_regions = data['region'].unique().tolist()
all_diseases = data['disease'].unique().tolist()

regions = st.sidebar.multiselect("Select Region(s):", options=all_regions, default=all_regions)
diseases = st.sidebar.multiselect("Select Disease(s):", options=all_diseases, default=all_diseases)
date_range = st.sidebar.date_input("Select Date Range:", [data['date'].min(), data['date'].max()])

# Handle cases when filters are empty
if not regions:
    regions = all_regions
if not diseases:
    diseases = all_diseases

# Filter data based on user selections
filtered_data = data[
    (data['region'].isin(regions)) &
    (data['disease'].isin(diseases)) &
    (data['date'] >= pd.to_datetime(date_range[0])) &
    (data['date'] <= pd.to_datetime(date_range[1]))
]


# -------------------------
# 3️⃣ Regional Summary Charts
# -------------------------
st.subheader("📈 Regional Summary")
summary = filtered_data.groupby("region")[["new_cases", "recovered", "deaths"]].sum().reset_index()
fig_region = px.bar(summary, x="region", y=["new_cases", "recovered", "deaths"], barmode="group",
                      title="Regional Summary")
st.plotly_chart(fig_region, use_container_width=True)

# -------------------------
# 4️⃣ Disease-wise Trends
# -------------------------
st.subheader("🦠 Disease-wise Cases")
disease_summary = filtered_data.groupby("disease")[["new_cases"]].sum().reset_index()
fig_disease = px.bar(disease_summary, x="disease", y="new_cases", color="new_cases",
                       title="Cases by Disease", color_continuous_scale='OrRd')
st.plotly_chart(fig_disease, use_container_width=True)

# -------------------------
# 5️⃣ Map Visualization
# -------------------------
st.subheader("🌍 Disease Hotspots Map")
if "latitude" in filtered_data.columns and "longitude" in filtered_data.columns:
    map_data = filtered_data.groupby(['region', 'latitude', 'longitude'])['new_cases'].sum().reset_index()

    # Custom color scale for the map points
    custom_color_scale = [
        [0.0, 'yellow'],    # Few cases
        [0.5, 'orange'],    # Medium cases
        [1.0, 'red']        # Severe cases
    ]

    fig_map = px.scatter_mapbox(
        map_data,
        lat="latitude",
        lon="longitude",
        size="new_cases",
        color="new_cases",
        hover_name="region",
        hover_data=["new_cases", "latitude", "longitude"],
        color_continuous_scale=custom_color_scale,
        size_max=15, # Adjusted for a clean look
        zoom=3
    )
    fig_map.update_layout(mapbox_style="open-street-map")
    fig_map.update_coloraxes(colorbar_title="New Cases Severity")
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.info("Map not shown: Please add 'latitude' and 'longitude' columns to your dataset.")

# -------------------------
# 6️⃣ SQL Query Panel (Moved Before the Live Stream)
# -------------------------
st.subheader("🔍 Run a Custom SQL Query")
query = st.text_input("Enter a SQL query:", "SELECT region, SUM(new_cases) AS total_cases FROM data GROUP BY region")
try:
    result = db_store.run_query(query)
    st.dataframe(result)
except Exception as e:
    st.error(f"❌ Query error: {e}")

# -------------------------
# 7️⃣ Live Streaming New Cases (Now at the very end)
# -------------------------
st.subheader("📊 Live New Cases Stream")
stream_placeholder = st.empty()
for chunk in stream_simulator.stream_data(filtered_data, chunk_size=10):
    fig_stream = px.bar(chunk.groupby("region")["new_cases"].sum().reset_index(),
                        x="region", y="new_cases", title="Live New Cases")
    stream_placeholder.plotly_chart(fig_stream, use_container_width=True)