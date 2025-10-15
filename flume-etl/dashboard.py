import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import boto3
import io
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="Project Flume - Weather & Water Dashboard",
    page_icon="🌊",
    layout="wide"
)

# S3 setup
S3_BUCKET = "project-nimbus-raw-data-lake-12345-raj"
s3_client = boto3.client('s3')

# Cache data loading (refresh every 5 minutes)
@st.cache_data(ttl=300)
def load_weather_station_latest():
    """Load latest weather conditions from Gold layer"""
    try:
        obj = s3_client.get_object(
            Bucket=S3_BUCKET, 
            Key='gold/weather_station_latest/weather_station_latest.parquet'
        )
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'])
        return df
    except Exception as e:
        st.error(f"Error loading weather data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_weather_hourly_summary():
    """Load hourly weather time series from Gold layer"""
    try:
        obj = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key='gold/weather_hourly_summary/weather_hourly_summary.parquet'
        )
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        df['hour'] = pd.to_datetime(df['hour'])
        return df
    except Exception as e:
        st.error(f"Error loading weather hourly data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_hydro_station_latest():
    """Load latest water levels from Gold layer"""
    try:
        obj = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key='gold/hydro_station_latest/hydro_station_latest.parquet'
        )
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'])
        return df
    except Exception as e:
        st.error(f"Error loading hydro data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_hydro_hourly_summary():
    """Load hourly water level time series from Gold layer"""
    try:
        obj = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key='gold/hydro_hourly_summary/hydro_hourly_summary.parquet'
        )
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        df['hour'] = pd.to_datetime(df['hour'])
        return df
    except Exception as e:
        st.error(f"Error loading hydro hourly data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_water_forecasts():
    """Load 6-hour water level forecasts"""
    try:
        obj = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key='gold/water_level_forecasts/6hour_forecast.parquet'
        )
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        df['hour'] = pd.to_datetime(df['hour'])
        return df
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_full_forecasts():
    """Load all 24-hour forecasts"""
    try:
        obj = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key='gold/water_level_forecasts/forecasts.parquet'
        )
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        df['hour'] = pd.to_datetime(df['hour'])
        return df
    except Exception as e:
        return pd.DataFrame()

# Load all data
with st.spinner('Loading data from S3...'):
    weather_data = load_weather_station_latest()
    weather_hourly = load_weather_hourly_summary()
    hydro_data = load_hydro_station_latest()
    hydro_hourly = load_hydro_hourly_summary()
    water_forecasts_6h = load_water_forecasts()
    water_forecasts_full = load_full_forecasts()

# Header
st.title("🌊 Project Flume - Weather & Water Monitoring Dashboard")
st.markdown("*Real-time observations from 2,600+ weather and water monitoring stations across Canada*")

# Create tabs
tab1, tab2 = st.tabs(["🌦️ Weather", "💧 Water Levels"])

# ============================================================
# TAB 1: WEATHER
# ============================================================

with tab1:
    # Check if data loaded
    if weather_data.empty:
        st.error("No weather data available. Please check S3 Gold layer.")
    else:
        # Sidebar filters
        st.sidebar.header("🔍 Weather Filters")
        
        # Temperature range filter
        temp_min = float(weather_data['air_temp'].min())
        temp_max = float(weather_data['air_temp'].max())
        temp_range = st.sidebar.slider(
            "Temperature Range (°C)",
            temp_min, temp_max,
            (temp_min, temp_max)
        )
        
        # Data freshness filter
        freshness_options = {
            "All data": 999999,
            "< 30 minutes": 30,
            "< 1 hour": 60,
            "< 2 hours": 120
        }
        freshness = st.sidebar.selectbox(
            "Data Freshness",
            list(freshness_options.keys())
        )
        
        # Apply filters
        filtered_weather = weather_data[
            (weather_data['air_temp'] >= temp_range[0]) &
            (weather_data['air_temp'] <= temp_range[1]) &
            (weather_data['minutes_since_update'] <= freshness_options[freshness])
        ].copy()
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Active Stations",
                f"{len(filtered_weather):,}",
                delta=f"{len(filtered_weather) - len(weather_data):,}" if len(filtered_weather) != len(weather_data) else None
            )
        
        with col2:
            avg_temp = filtered_weather['air_temp'].mean()
            st.metric(
                "Average Temperature",
                f"{avg_temp:.1f}°C"
            )
        
        with col3:
            max_temp = filtered_weather['air_temp'].max()
            hottest_station = filtered_weather[filtered_weather['air_temp'] == max_temp]['station_name'].iloc[0]
            st.metric(
                "Hottest",
                f"{max_temp:.1f}°C",
                delta=hottest_station
            )
        
        with col4:
            min_temp = filtered_weather['air_temp'].min()
            coldest_station = filtered_weather[filtered_weather['air_temp'] == min_temp]['station_name'].iloc[0]
            st.metric(
                "Coldest",
                f"{min_temp:.1f}°C",
                delta=coldest_station
            )
        
        st.divider()
        
        # Main weather map
        st.subheader("📍 Weather Station Map")
        
        # Calculate absolute size for markers
        filtered_weather['marker_size'] = filtered_weather['air_temp'].abs() + 5
        
        # Create map
        fig = px.scatter_mapbox(
            filtered_weather,
            lat='latitude',
            lon='longitude',
            color='air_temp',
            size='marker_size',
            hover_name='station_name',
            hover_data={
                'air_temp': ':.1f',
                'feels_like_temp': ':.1f',
                'rel_hum': ':.0f',
                'wind_speed': ':.1f',
                'latitude': False,
                'longitude': False,
                'marker_size': False
            },
            color_continuous_scale='RdYlBu_r',
            size_max=15,
            zoom=3,
            height=500,
            labels={
                'air_temp': 'Temp (°C)',
                'feels_like_temp': 'Feels Like (°C)',
                'rel_hum': 'Humidity (%)',
                'wind_speed': 'Wind (km/h)'
            }
        )
        
        fig.update_layout(
            mapbox_style="open-street-map",
            margin={"r":0,"t":0,"l":0,"b":0}
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        
        # Station selection and details
        st.subheader("📊 Weather Station Details")
        
        # Station selector
        station_list = sorted(filtered_weather['station_name'].unique())
        selected_station = st.selectbox(
            "Select a weather station",
            station_list,
            index=0 if station_list else None,
            key="weather_station"
        )
        
        if selected_station:
            # Get station data
            station_info = filtered_weather[filtered_weather['station_name'] == selected_station].iloc[0]
            
            # Station info cards
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Temperature", f"{station_info['air_temp']:.1f}°C")
            with col2:
                st.metric("Feels Like", f"{station_info['feels_like_temp']:.1f}°C")
            with col3:
                st.metric("Humidity", f"{station_info['rel_hum']:.0f}%")
            with col4:
                st.metric("Wind Speed", f"{station_info['wind_speed']:.1f} km/h")
            
            # Time series chart
            if not weather_hourly.empty:
                station_hourly = weather_hourly[
                    weather_hourly['station_name'] == selected_station
                ].sort_values('hour')
                
                if not station_hourly.empty:
                    st.subheader("📈 Temperature Trend")
                    
                    fig_temp = go.Figure()
                    
                    fig_temp.add_trace(go.Scatter(
                        x=station_hourly['hour'],
                        y=station_hourly['temp_mean'],
                        mode='lines+markers',
                        name='Temperature',
                        line=dict(color='red', width=2),
                        fill='tozeroy'
                    ))
                    
                    fig_temp.update_layout(
                        xaxis_title="Time",
                        yaxis_title="Temperature (°C)",
                        height=300,
                        margin=dict(l=0, r=0, t=20, b=0)
                    )
                    
                    st.plotly_chart(fig_temp, use_container_width=True)
        
        st.divider()
        
        # Weather alerts
        st.subheader("⚠️ Weather Alerts")
        
        hot_threshold = 30
        cold_threshold = -10
        
        hot_stations = filtered_weather[filtered_weather['air_temp'] > hot_threshold]
        cold_stations = filtered_weather[filtered_weather['air_temp'] < cold_threshold]
        
        col1, col2 = st.columns(2)
        
        with col1:
            if not hot_stations.empty:
                st.warning(f"🔥 **{len(hot_stations)} stations** reporting temperatures above {hot_threshold}°C")
                with st.expander("View hot stations"):
                    st.dataframe(
                        hot_stations[['station_name', 'air_temp', 'feels_like_temp']].sort_values('air_temp', ascending=False),
                        hide_index=True
                    )
            else:
                st.success("No heat warnings")
        
        with col2:
            if not cold_stations.empty:
                st.warning(f"❄️ **{len(cold_stations)} stations** reporting temperatures below {cold_threshold}°C")
                with st.expander("View cold stations"):
                    st.dataframe(
                        cold_stations[['station_name', 'air_temp', 'feels_like_temp']].sort_values('air_temp'),
                        hide_index=True
                    )
            else:
                st.success("No cold warnings")

# ============================================================
# TAB 2: WATER LEVELS
# ============================================================

with tab2:
    # Check if data loaded
    if hydro_data.empty:
        st.error("No hydrometric data available. Please check S3 Gold layer.")
    else:
        # Sidebar filters for water
        st.sidebar.header("🔍 Water Level Filters")
        
        # Province filter
        provinces = ['All'] + sorted(hydro_data['province'].unique().tolist())
        selected_province = st.sidebar.selectbox(
            "Province",
            provinces
        )
        
        # Water level range filter
        valid_water_levels = hydro_data['water_level'].dropna()
        if not valid_water_levels.empty:
            level_min = float(valid_water_levels.min())
            level_max = float(valid_water_levels.max())
            level_range = st.sidebar.slider(
                "Water Level Range (m)",
                level_min, level_max,
                (level_min, level_max)
            )
        else:
            level_range = (0, 100)
        
        # Apply filters
        filtered_hydro = hydro_data.copy()
        if selected_province != 'All':
            filtered_hydro = filtered_hydro[filtered_hydro['province'] == selected_province]
        
        filtered_hydro = filtered_hydro[
            (filtered_hydro['water_level'] >= level_range[0]) &
            (filtered_hydro['water_level'] <= level_range[1])
        ]
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Active Stations",
                f"{len(filtered_hydro):,}"
            )
        
        with col2:
            avg_level = filtered_hydro['water_level'].mean()
            st.metric(
                "Average Water Level",
                f"{avg_level:.2f} m"
            )
        
        with col3:
            max_level = filtered_hydro['water_level'].max()
            highest_station = filtered_hydro[filtered_hydro['water_level'] == max_level]['station_name'].iloc[0]
            st.metric(
                "Highest Level",
                f"{max_level:.2f} m",
                delta=highest_station
            )
        
        with col4:
            min_level = filtered_hydro['water_level'].min()
            lowest_station = filtered_hydro[filtered_hydro['water_level'] == min_level]['station_name'].iloc[0]
            st.metric(
                "Lowest Level",
                f"{min_level:.2f} m",
                delta=lowest_station
            )
        
        st.divider()
        
        # Forecast Summary (NEW!)
        if not water_forecasts_6h.empty:
            st.subheader("🔮 6-Hour Forecast Summary")
            
            col1, col2, col3, col4 = st.columns(4)
            
            # Apply same province filter to forecasts
            filtered_forecasts = water_forecasts_6h.copy()
            if selected_province != 'All':
                filtered_forecasts = filtered_forecasts[filtered_forecasts['province'] == selected_province]
            
            with col1:
                rising = len(filtered_forecasts[filtered_forecasts['forecast_change'] > 0.1])
                st.metric(
                    "🔼 Rising Levels",
                    rising,
                    delta=f"+{rising} stations"
                )
            
            with col2:
                falling = len(filtered_forecasts[filtered_forecasts['forecast_change'] < -0.1])
                st.metric(
                    "🔽 Falling Levels", 
                    falling,
                    delta=f"-{falling} stations"
                )
            
            with col3:
                stable = len(filtered_forecasts[filtered_forecasts['forecast_change'].abs() <= 0.1])
                st.metric(
                    "➡️ Stable Levels",
                    stable
                )
            
            with col4:
                high_risk = len(filtered_forecasts[filtered_forecasts['forecast_change'] > 0.5])
                if high_risk > 0:
                    st.metric(
                        "⚠️ High Risk",
                        high_risk,
                        delta="Flood watch",
                        delta_color="inverse"
                    )
                else:
                    st.metric(
                        "✅ Low Risk",
                        "0",
                        delta="All clear"
                    )
        
        st.divider()
        
        # Main water level map
        st.subheader("📍 Water Monitoring Station Map")
        
        # Calculate marker size based on water level
        filtered_hydro['marker_size'] = filtered_hydro['water_level'].abs() + 3
        
        # Create map
        fig_hydro = px.scatter_mapbox(
            filtered_hydro,
            lat='latitude',
            lon='longitude',
            color='water_level',
            size='marker_size',
            hover_name='station_name',
            hover_data={
                'station_number': True,
                'province': True,
                'water_level': ':.2f',
                'discharge': ':.2f',
                'latitude': False,
                'longitude': False,
                'marker_size': False
            },
            color_continuous_scale='Blues',
            size_max=12,
            zoom=3,
            height=500,
            labels={
                'water_level': 'Water Level (m)',
                'discharge': 'Discharge (m³/s)',
                'station_number': 'Station ID'
            }
        )
        
        fig_hydro.update_layout(
            mapbox_style="open-street-map",
            margin={"r":0,"t":0,"l":0,"b":0}
        )
        
        st.plotly_chart(fig_hydro, use_container_width=True)
        
        st.divider()
        
        # Station selection and details
        st.subheader("📊 Water Station Details")
        
        # Station selector
        hydro_station_list = sorted(filtered_hydro['station_name'].unique())
        selected_hydro_station = st.selectbox(
            "Select a water monitoring station",
            hydro_station_list,
            index=0 if hydro_station_list else None,
            key="hydro_station"
        )
        
        if selected_hydro_station:
            # Get station data
            hydro_station_info = filtered_hydro[filtered_hydro['station_name'] == selected_hydro_station].iloc[0]
            
            # Station info cards
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Water Level", f"{hydro_station_info['water_level']:.2f} m")
            with col2:
                discharge_val = hydro_station_info['discharge']
                if pd.notna(discharge_val):
                    st.metric("Discharge", f"{discharge_val:.2f} m³/s")
                else:
                    st.metric("Discharge", "N/A")
            with col3:
                st.metric("Province", hydro_station_info['province'])
            with col4:
                st.metric("Station ID", hydro_station_info['station_number'])
            
            # Time series chart with forecast (ENHANCED!)
            if not hydro_hourly.empty or not water_forecasts_full.empty:
                st.subheader("📈 Water Level Trend & Forecast")
                
                fig_water = go.Figure()
                
                # Historical data
                if not hydro_hourly.empty:
                    station_hydro_hourly = hydro_hourly[
                        hydro_hourly['station_name'] == selected_hydro_station
                    ].sort_values('hour')
                    
                    if not station_hydro_hourly.empty:
                        fig_water.add_trace(go.Scatter(
                            x=station_hydro_hourly['hour'],
                            y=station_hydro_hourly['water_level_mean'],
                            mode='lines+markers',
                            name='Historical',
                            line=dict(color='blue', width=2),
                            marker=dict(size=6)
                        ))
                
                # Forecast data (NEW!)
                if not water_forecasts_full.empty:
                    station_forecast = water_forecasts_full[
                        water_forecasts_full['station_name'] == selected_hydro_station
                    ].sort_values('hour')
                    
                    if not station_forecast.empty:
                        # Predicted line
                        fig_water.add_trace(go.Scatter(
                            x=station_forecast['hour'],
                            y=station_forecast['predicted_water_level'],
                            mode='lines+markers',
                            name='Forecast',
                            line=dict(color='red', width=2, dash='dash'),
                            marker=dict(size=6, symbol='diamond')
                        ))
                        
                        # Confidence interval
                        fig_water.add_trace(go.Scatter(
                            x=station_forecast['hour'],
                            y=station_forecast['confidence_upper'],
                            mode='lines',
                            name='Upper Bound',
                            line=dict(width=0),
                            showlegend=False
                        ))
                        
                        fig_water.add_trace(go.Scatter(
                            x=station_forecast['hour'],
                            y=station_forecast['confidence_lower'],
                            mode='lines',
                            name='Confidence Interval',
                            fill='tonexty',
                            fillcolor='rgba(255, 0, 0, 0.1)',
                            line=dict(width=0)
                        ))
                
                fig_water.update_layout(
                    xaxis_title="Time",
                    yaxis_title="Water Level (m)",
                    height=400,
                    margin=dict(l=0, r=0, t=20, b=0),
                    hovermode='x unified'
                )
                
                st.plotly_chart(fig_water, use_container_width=True)
                
                # Forecast alert (NEW!)
                if not water_forecasts_6h.empty:
                    station_forecast_6h = water_forecasts_6h[
                        water_forecasts_6h['station_name'] == selected_hydro_station
                    ]
                    
                    if not station_forecast_6h.empty:
                        forecast_info = station_forecast_6h.iloc[0]
                        change = forecast_info['forecast_change']
                        
                        if abs(change) > 0.5:
                            direction = "RISING" if change > 0 else "FALLING"
                            st.error(f"🚨 **ALERT:** Water level predicted to {direction} by **{abs(change):.2f}m** in next 6 hours!")
                        elif abs(change) > 0.1:
                            direction = "rise" if change > 0 else "fall"
                            st.info(f"ℹ️ Water level expected to {direction} by {abs(change):.2f}m in next 6 hours")
                        else:
                            st.success(f"✅ Water level expected to remain stable (±{abs(change):.2f}m)")
        
        st.divider()
        
        # Water level alerts
        st.subheader("⚠️ Water Level Alerts")
        
        # Simple threshold-based alerts
        high_threshold = filtered_hydro['water_level'].quantile(0.9)
        low_threshold = filtered_hydro['water_level'].quantile(0.1)
        
        high_stations = filtered_hydro[filtered_hydro['water_level'] > high_threshold]
        low_stations = filtered_hydro[filtered_hydro['water_level'] < low_threshold]
        
        col1, col2 = st.columns(2)
        
        with col1:
            if not high_stations.empty:
                st.warning(f"⚠️ **{len(high_stations)} stations** reporting high water levels (top 10%)")
                with st.expander("View high water stations"):
                    st.dataframe(
                        high_stations[['station_name', 'province', 'water_level']].sort_values('water_level', ascending=False).head(10),
                        hide_index=True
                    )
            else:
                st.success("No high water warnings")
        
        with col2:
            if not low_stations.empty:
                st.info(f"ℹ️ **{len(low_stations)} stations** reporting low water levels (bottom 10%)")
                with st.expander("View low water stations"):
                    st.dataframe(
                        low_stations[['station_name', 'province', 'water_level']].sort_values('water_level').head(10),
                        hide_index=True
                    )
            else:
                st.success("No low water warnings")

# Footer
st.divider()
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data source: Environment Canada")
st.caption(f"Weather: {len(weather_data):,} stations | Water: {len(hydro_data):,} stations")