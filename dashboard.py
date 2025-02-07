import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import json
from datetime import datetime, timedelta
import numpy as np
import folium
from streamlit_folium import folium_static
from folium.plugins import HeatMap

class QatarTrafficDashboard:
    def __init__(self):
        st.set_page_config(page_title="Qatar Network Traffic Analysis", layout="wide")
        self.traffic_data = None
        self.events_data = None
        self.qatar_locations = {
            'Doha': [25.2854, 51.5310],
            'Al Wakrah': [25.1715, 51.6034],
            'Al Khor': [25.6840, 51.4978],
            'Lusail': [25.4285, 51.4877],
            'West Bay': [25.3287, 51.5295]
        }

    def load_data(self):
        """Load traffic and events data"""
        try:
            # Load traffic data from CSV files
            traffic_files = [f'traffic_data_batch_202401{i:02d}.csv' for i in range(1, 32)]
            dfs = []
            for file in traffic_files:
                try:
                    df = pd.read_csv(file)
                    dfs.append(df)
                except FileNotFoundError:
                    continue
            self.traffic_data = pd.concat(dfs, ignore_index=True)
            self.traffic_data['timestamp'] = pd.to_datetime(self.traffic_data['timestamp'])
            
            # Load events data
            with open('processed_events.json', 'r') as f:
                self.events_data = pd.DataFrame(json.load(f))
            
            # Process event dates
            self.events_data['date'] = pd.to_datetime(
                self.events_data['date_range'].str.split('-').str[0].str.strip()
            )
            
            return True
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            return False

    def create_header(self):
        """Create dashboard header"""
        st.title("Qatar Network Traffic Analysis Dashboard")
        st.markdown("""
        This dashboard visualizes network traffic patterns in Qatar and their correlation with major events.
        Use the filters below to explore different time periods and metrics.
        """)

    def create_filters(self):
        """Create dashboard filters"""
        col1, col2 = st.columns(2)
        
        with col1:
            start_date = st.date_input(
                "Start Date",
                min(self.traffic_data['timestamp']).date()
            )
        
        with col2:
            end_date = st.date_input(
                "End Date",
                max(self.traffic_data['timestamp']).date()
            )
            
        return start_date, end_date

    def plot_traffic_overview(self, start_date, end_date):
        """Create traffic overview plot"""
        st.subheader("Network Traffic Overview")
        
        # Filter data
        mask = (self.traffic_data['timestamp'].dt.date >= start_date) & \
               (self.traffic_data['timestamp'].dt.date <= end_date)
        filtered_data = self.traffic_data[mask]
        
        # Aggregate by hour
        hourly_data = filtered_data.groupby(
            filtered_data['timestamp'].dt.floor('h')
        ).agg({
            'bytes': 'sum',
            'packets': 'sum',
            'latency': 'mean',
            'packet_loss': 'mean'
        }).reset_index()
        
        # Create figure with secondary y-axis
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add traces
        fig.add_trace(
            go.Scatter(
                x=hourly_data['timestamp'],
                y=hourly_data['bytes'] / 1e9,  # Convert to GB
                name="Traffic Volume (GB)",
                line=dict(color='blue')
            ),
            secondary_y=False
        )
        
        fig.add_trace(
            go.Scatter(
                x=hourly_data['timestamp'],
                y=hourly_data['latency'],
                name="Latency (ms)",
                line=dict(color='red')
            ),
            secondary_y=True
        )
        
        # Add event markers
        events_in_range = self.events_data[
            (self.events_data['date'].dt.date >= start_date) &
            (self.events_data['date'].dt.date <= end_date)
        ]
        
        for _, event in events_in_range.iterrows():
            fig.add_vline(
                x=event['date'],
                line_dash="dash",
                annotation_text=event['event_name'][:30] + "...",
                annotation_position="top right"
            )
        
        # Update layout
        fig.update_layout(
            title="Network Traffic and Latency Over Time",
            xaxis_title="Time",
            yaxis_title="Traffic Volume (GB)",
            yaxis2_title="Latency (ms)",
            hovermode='x unified',
            height=600
        )
        
        st.plotly_chart(fig, use_container_width=True)

    def plot_geographical_heatmap(self):
        """Create geographical heatmap of network activity"""
        st.subheader("Geographical Network Activity")
        
        m = folium.Map(location=[25.3548, 51.1839], zoom_start=10, tiles='cartodbpositron')
        heat_data = []

        for location, coords in self.qatar_locations.items():
            location_traffic = self.traffic_data[
                self.traffic_data['dest_ip'].str.startswith(str(coords[0])[:6])
            ]['bytes'].sum()
            
            heat_data.append([coords[0], coords[1], location_traffic / 1e12])  # Weight by TB
        
        HeatMap(heat_data).add_to(m)
        folium_static(m)

    def plot_protocol_distribution(self):
        """Create protocol distribution visualization"""
        st.subheader("Protocol Distribution Analysis")
        
        protocol_metrics = self.traffic_data.groupby('protocol').agg({
            'bytes': 'sum',
            'packets': 'sum',
            'latency': 'mean'
        }).reset_index()
        
        fig = go.Figure(data=[go.Pie(
            labels=protocol_metrics['protocol'],
            values=protocol_metrics['bytes'],
            hole=.3
        )])
        
        fig.update_layout(title_text="Traffic Distribution by Protocol")
        st.plotly_chart(fig, use_container_width=True)

    def run_dashboard(self):
        """Main dashboard execution"""
        self.create_header()
        
        if self.load_data():
            start_date, end_date = self.create_filters()
            self.plot_traffic_overview(start_date, end_date)
            
            col1, col2 = st.columns(2)
            with col1:
                self.plot_protocol_distribution()
            with col2:
                self.plot_geographical_heatmap()

if __name__ == "__main__":
    dashboard = QatarTrafficDashboard()
    dashboard.run_dashboard()
