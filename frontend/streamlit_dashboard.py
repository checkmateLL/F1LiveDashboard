import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
import fastf1
import fastf1.plotting
from datetime import datetime
import numpy as np

# Set page configuration
st.set_page_config(
    page_title="F1 Data Dashboard",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configure FastF1 cache
if not os.path.exists("./fastf1_cache"):
    os.makedirs("./fastf1_cache")
fastf1.Cache.enable_cache("./fastf1_cache")

# Database connection
def get_db_connection(db_path='./f1_data.db'):
    """Create a database connection to the SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        st.error(f"Error connecting to database: {e}")
        return None

# Check if database exists
if not os.path.exists('./f1_data.db'):
    st.warning("Database not found! Please run the migration script first.")
    st.code("python migrate_sqlite.py 2023", language="bash")
    st.stop()
    
# Database is available, continue with the dashboard
conn = get_db_connection()

# Define CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 600;
        color: #ff1801;
        text-align: center;
        margin-bottom: 1rem;
    }
    .section-header {
        font-size: 1.8rem;
        font-weight: 500;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
    }
    .card {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8f9fa;
        box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
        margin-bottom: 1rem;
    }
    .metric-card {
        text-align: center;
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8f9fa;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
    }
    .metric-label {
        font-size: 1rem;
        color: #6c757d;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("<h1 class='main-header'>Formula 1 Data Dashboard</h1>", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("Navigation")
page = st.sidebar.radio("Select a page", ["Season Overview", "Race Analysis", "Driver Comparison", "Telemetry Analysis"])

# Get available years from database
cursor = conn.cursor()
cursor.execute("SELECT DISTINCT year FROM events ORDER BY year DESC")
available_years = [row[0] for row in cursor.fetchall()]

if not available_years:
    st.error("No data found in the database. Please run the migration script first.")
    st.stop()

selected_year = st.sidebar.selectbox("Select Year", available_years)

# Function to get team color mapping
def get_team_colors(year):
    """Get a mapping of teams to their colors for a specific year"""
    cursor = conn.cursor()
    cursor.execute("SELECT name, team_color FROM teams WHERE year = ?", (year,))
    return {row[0]: row[1] for row in cursor.fetchall()}

# Function to get driver abbreviations
def get_driver_abbr_mapping(year):
    """Get a mapping of driver IDs to abbreviations for a specific year"""
    cursor = conn.cursor()
    cursor.execute("SELECT id, abbreviation, full_name FROM drivers WHERE year = ?", (year,))
    return {row[0]: {"abbr": row[1], "name": row[2]} for row in cursor.fetchall()}

# Main content based on selected page
if page == "Season Overview":
    st.markdown("<h2 class='section-header'>Season Overview</h2>", unsafe_allow_html=True)
    
    # Get races for the selected year
    cursor.execute("""
        SELECT e.id, e.round_number, e.country, e.location, e.event_name, e.event_date
        FROM events e
        WHERE e.year = ?
        ORDER BY e.round_number
    """, (selected_year,))
    races = cursor.fetchall()
    
    # Display race calendar
    st.markdown("<h3>Race Calendar</h3>", unsafe_allow_html=True)
    
    race_data = []
    for race in races:
        race_date = datetime.fromisoformat(race['event_date']) if race['event_date'] else None
        race_data.append({
            'Round': race['round_number'],
            'Country': race['country'],
            'Location': race['location'],
            'Grand Prix': race['event_name'],
            'Date': race_date.strftime('%d %b %Y') if race_date else 'N/A'
        })
    
    race_df = pd.DataFrame(race_data)
    st.dataframe(race_df, use_container_width=True)
    
    # Driver Standings
    st.markdown("<h3>Driver Standings</h3>", unsafe_allow_html=True)
    
    # Get driver standings from race results
    cursor.execute("""
        SELECT d.id, d.full_name, d.abbreviation, t.name as team_name, t.team_color,
               SUM(r.points) as total_points
        FROM drivers d
        JOIN teams t ON d.team_id = t.id
        JOIN results r ON d.id = r.driver_id
        JOIN sessions s ON r.session_id = s.id
        JOIN events e ON s.event_id = e.id
        WHERE e.year = ? AND s.session_type = 'race'
        GROUP BY d.id
        ORDER BY total_points DESC
    """, (selected_year,))
    
    driver_standings = cursor.fetchall()
    
    if driver_standings:
        driver_data = []
        for i, driver in enumerate(driver_standings):
            driver_data.append({
                'Position': i + 1,
                'Driver': driver['full_name'],
                'Abbreviation': driver['abbreviation'],
                'Team': driver['team_name'],
                'Points': driver['total_points']
            })
        
        driver_df = pd.DataFrame(driver_data)
        
        fig = px.bar(
            driver_df, 
            x='Driver', 
            y='Points',
            color='Team',
            text='Points',
            title=f"{selected_year} Driver Standings",
            labels={'Points': 'Points', 'Driver': 'Driver'},
            height=500
        )
        
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        
        # Show standings as a table as well
        st.dataframe(driver_df, use_container_width=True)
    else:
        st.info(f"No race results available for {selected_year}")
    
    # Constructor Standings
    st.markdown("<h3>Constructor Standings</h3>", unsafe_allow_html=True)
    
    # Get constructor standings from race results
    cursor.execute("""
        SELECT t.name as team_name, t.team_color,
               SUM(r.points) as total_points
        FROM teams t
        JOIN drivers d ON t.id = d.team_id
        JOIN results r ON d.id = r.driver_id
        JOIN sessions s ON r.session_id = s.id
        JOIN events e ON s.event_id = e.id
        WHERE e.year = ? AND s.session_type = 'race'
        GROUP BY t.id
        ORDER BY total_points DESC
    """, (selected_year,))
    
    team_standings = cursor.fetchall()
    
    if team_standings:
        team_data = []
        for i, team in enumerate(team_standings):
            team_data.append({
                'Position': i + 1,
                'Team': team['team_name'],
                'Points': team['total_points'],
                'Color': team['team_color']
            })
        
        team_df = pd.DataFrame(team_data)
        
        fig = px.bar(
            team_df, 
            x='Team', 
            y='Points',
            color='Team',
            text='Points',
            color_discrete_map={team: color for team, color in zip(team_df['Team'], team_df['Color'])},
            title=f"{selected_year} Constructor Standings",
            labels={'Points': 'Points', 'Team': 'Team'},
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show standings as a table
        st.dataframe(team_df.drop(columns=['Color']), use_container_width=True)
    else:
        st.info(f"No race results available for {selected_year}")

elif page == "Race Analysis":
    st.markdown("<h2 class='section-header'>Race Analysis</h2>", unsafe_allow_html=True)
    
    # Get races for the selected year
    cursor.execute("""
        SELECT e.id, e.round_number, e.event_name
        FROM events e
        WHERE e.year = ?
        ORDER BY e.round_number
    """, (selected_year,))
    races = cursor.fetchall()
    
    race_options = [f"Round {race['round_number']} - {race['event_name']}" for race in races]
    
    selected_race_index = st.selectbox("Select Race", range(len(race_options)), format_func=lambda i: race_options[i])
    selected_race = races[selected_race_index]
    
    # Get race session ID
    cursor.execute("""
        SELECT s.id
        FROM sessions s
        JOIN events e ON s.event_id = e.id
        WHERE e.id = ? AND s.session_type = 'race'
    """, (selected_race['id'],))
    race_session = cursor.fetchone()
    
    if race_session:
        race_session_id = race_session['id']
        
        # Race Results
        cursor.execute("""
            SELECT r.position, d.full_name as driver_name, d.abbreviation, t.name as team_name, 
                   r.grid_position, r.status, r.points, r.race_time
            FROM results r
            JOIN drivers d ON r.driver_id = d.id
            JOIN teams t ON d.team_id = t.id
            WHERE r.session_id = ?
            ORDER BY 
                CASE WHEN r.position IS NULL THEN 999 ELSE r.position END
        """, (race_session_id,))
        
        race_results = cursor.fetchall()
        
        if race_results:
            st.markdown(f"<h3>Race Results - {selected_race['event_name']}</h3>", unsafe_allow_html=True)
            
            results_data = []
            for result in race_results:
                results_data.append({
                    'Pos': result['position'] if result['position'] else 'DNF',
                    'Driver': result['driver_name'],
                    'Abbreviation': result['abbreviation'],
                    'Team': result['team_name'],
                    'Grid': result['grid_position'],
                    'Status': result['status'],
                    'Points': result['points'],
                    'Race Time': result['race_time'] if result['race_time'] else 'N/A'
                })
            
            results_df = pd.DataFrame(results_data)
            st.dataframe(results_df, use_container_width=True)
            
            # Grid vs. Finish Position Chart
            st.markdown("<h3>Grid vs. Finish Position</h3>", unsafe_allow_html=True)
            
            # Filter out DNFs for this chart
            grid_finish_df = results_df[results_df['Pos'] != 'DNF'].copy()
            grid_finish_df['Pos'] = grid_finish_df['Pos'].astype(int)
            grid_finish_df['Grid'] = grid_finish_df['Grid'].astype(int)
            
            # Calculate positions gained/lost
            grid_finish_df['Positions Gained'] = grid_finish_df['Grid'] - grid_finish_df['Pos']
            
            # Sort by positions gained
            grid_finish_df = grid_finish_df.sort_values('Positions Gained', ascending=False)
            
            # Create a color scale for positions gained/lost
            colors = ['red' if x < 0 else 'green' for x in grid_finish_df['Positions Gained']]
            
            # Create the visualization
            fig = go.Figure()
            
            # Add grid positions
            fig.add_trace(go.Bar(
                x=grid_finish_df['Driver'],
                y=grid_finish_df['Grid'],
                name='Grid Position',
                marker_color='blue',
                opacity=0.6
            ))
            
            # Add finish positions
            fig.add_trace(go.Bar(
                x=grid_finish_df['Driver'],
                y=grid_finish_df['Pos'],
                name='Finish Position',
                marker_color='orange',
                opacity=0.6
            ))
            
            # Add lines connecting grid and finish positions
            for i, row in grid_finish_df.iterrows():
                fig.add_shape(
                    type="line",
                    x0=i, y0=row['Grid'],
                    x1=i, y1=row['Pos'],
                    line=dict(color=colors[i], width=2),
                    xref="x", yref="y"
                )
            
            # Invert y-axis so that position 1 is at the top
            fig.update_layout(
                title=f"Grid vs. Finish Position - {selected_race['event_name']}",
                xaxis_title="Driver",
                yaxis_title="Position",
                yaxis=dict(autorange="reversed"),
                barmode='overlay',
                height=600
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Lap Times Analysis
            st.markdown("<h3>Lap Times Analysis</h3>", unsafe_allow_html=True)
            
            # Get drivers for multi-select
            driver_abbrs = [result['abbreviation'] for result in race_results]
            selected_drivers = st.multiselect("Select Drivers", driver_abbrs, default=driver_abbrs[:3])
            
            if selected_drivers:
                # Get lap times for selected drivers
                lap_times_data = []
                
                for driver_abbr in selected_drivers:
                    cursor.execute("""
                        SELECT l.lap_number, l.lap_time, d.abbreviation, t.team_color
                        FROM laps l
                        JOIN drivers d ON l.driver_id = d.id
                        JOIN teams t ON d.team_id = t.id
                        WHERE l.session_id = ? AND d.abbreviation = ? AND l.lap_time IS NOT NULL
                        ORDER BY l.lap_number
                    """, (race_session_id, driver_abbr))
                    
                    driver_laps = cursor.fetchall()
                    
                    for lap in driver_laps:
                        # Parse lap time (format: 0 days 00:01:30.000000)
                        lap_time_str = lap['lap_time']
                        if lap_time_str and "days" in lap_time_str:
                            time_parts = lap_time_str.split()
                            if len(time_parts) >= 3:
                                time_str = time_parts[2]
                                # Convert to seconds
                                h, m, s = time_str.split(':')
                                lap_time_sec = int(h) * 3600 + int(m) * 60 + float(s)
                                
                                lap_times_data.append({
                                    'Driver': lap['abbreviation'],
                                    'Lap': lap['lap_number'],
                                    'LapTime': lap_time_sec,
                                    'Color': lap['team_color']
                                })
                
                if lap_times_data:
                    lap_times_df = pd.DataFrame(lap_times_data)
                    
                    # Create line chart of lap times
                    fig = px.line(
                        lap_times_df,
                        x='Lap',
                        y='LapTime',
                        color='Driver',
                        title=f"Lap Times - {selected_race['event_name']}",
                        labels={'LapTime': 'Lap Time (seconds)', 'Lap': 'Lap Number'},
                        color_discrete_map={driver: color for driver, color in zip(lap_times_df['Driver'].unique(), lap_times_df['Color'].unique())},
                        height=500
                    )
                    
                    # Improve readability
                    fig.update_layout(
                        xaxis=dict(dtick=5),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No lap time data available for the selected drivers")
            else:
                st.info("Please select at least one driver")
        else:
            st.info(f"No race results available for {selected_race['event_name']}")
    else:
        st.info(f"No race session found for {selected_race['event_name']}")

elif page == "Driver Comparison":
    st.markdown("<h2 class='section-header'>Driver Comparison</h2>", unsafe_allow_html=True)
    
    # Get all drivers for the selected year
    cursor.execute("""
        SELECT d.id, d.full_name, d.abbreviation, t.name as team_name, t.team_color
        FROM drivers d
        JOIN teams t ON d.team_id = t.id
        WHERE d.year = ?
        ORDER BY t.name, d.full_name
    """, (selected_year,))
    
    all_drivers = cursor.fetchall()
    
    driver_options = [f"{driver['abbreviation']} - {driver['full_name']} ({driver['team_name']})" for driver in all_drivers]
    
    col1, col2 = st.columns(2)
    
    with col1:
        driver1_index = st.selectbox("Select Driver 1", range(len(driver_options)), format_func=lambda i: driver_options[i], key="driver1")
    
    with col2:
        # Set a default that's different from driver1
        default_driver2 = 1 if driver1_index == 0 else 0
        driver2_index = st.selectbox("Select Driver 2", range(len(driver_options)), 
                                   format_func=lambda i: driver_options[i], 
                                   index=default_driver2, key="driver2")
    
    driver1 = all_drivers[driver1_index]
    driver2 = all_drivers[driver2_index]
    
    # Get all races for comparison
    cursor.execute("""
        SELECT e.id, e.round_number, e.event_name
        FROM events e
        WHERE e.year = ?
        ORDER BY e.round_number
    """, (selected_year,))
    
    races = cursor.fetchall()
    
    if races:
        st.markdown("<h3>Season Performance Comparison</h3>", unsafe_allow_html=True)
        
        # Get race results for both drivers
        performance_data = []
        
        for race in races:
            # Get race session ID
            cursor.execute("""
                SELECT s.id
                FROM sessions s
                JOIN events e ON s.event_id = e.id
                WHERE e.id = ? AND s.session_type = 'race'
            """, (race['id'],))
            
            race_session = cursor.fetchone()
            
            if race_session:
                race_session_id = race_session['id']
                
                # Driver 1 result
                cursor.execute("""
                    SELECT r.position, r.grid_position, r.points
                    FROM results r
                    WHERE r.session_id = ? AND r.driver_id = ?
                """, (race_session_id, driver1['id']))
                
                driver1_result = cursor.fetchone()
                
                # Driver 2 result
                cursor.execute("""
                    SELECT r.position, r.grid_position, r.points
                    FROM results r
                    WHERE r.session_id = ? AND r.driver_id = ?
                """, (race_session_id, driver2['id']))
                
                driver2_result = cursor.fetchone()
                
                if driver1_result and driver2_result:
                    performance_data.append({
                        'Race': f"R{race['round_number']} - {race['event_name']}",
                        f"{driver1['abbreviation']} Grid": driver1_result['grid_position'],
                        f"{driver1['abbreviation']} Finish": driver1_result['position'],
                        f"{driver1['abbreviation']} Points": driver1_result['points'],
                        f"{driver2['abbreviation']} Grid": driver2_result['grid_position'],
                        f"{driver2['abbreviation']} Finish": driver2_result['position'],
                        f"{driver2['abbreviation']} Points": driver2_result['points']
                    })
        
        if performance_data:
            performance_df = pd.DataFrame(performance_data)
            
            # Calculate cumulative points
            performance_df[f"{driver1['abbreviation']} Cum. Points"] = performance_df[f"{driver1['abbreviation']} Points"].cumsum()
            performance_df[f"{driver2['abbreviation']} Cum. Points"] = performance_df[f"{driver2['abbreviation']} Points"].cumsum()
            
            # Display the performance comparison table
            st.dataframe(performance_df, use_container_width=True)
            
            # Cumulative Points Chart
            st.markdown("<h3>Cumulative Points Comparison</h3>", unsafe_allow_html=True)
            
            # Prepare data for the line chart
            cum_points_data = []
            
            for i, row in performance_df.iterrows():
                cum_points_data.append({
                    'Race': row['Race'],
                    'Driver': driver1['abbreviation'],
                    'Points': row[f"{driver1['abbreviation']} Cum. Points"],
                    'Color': driver1['team_color']
                })
                
                cum_points_data.append({
                    'Race': row['Race'],
                    'Driver': driver2['abbreviation'],
                    'Points': row[f"{driver2['abbreviation']} Cum. Points"],
                    'Color': driver2['team_color']
                })
            
            cum_points_df = pd.DataFrame(cum_points_data)
            
            fig = px.line(
                cum_points_df,
                x='Race',
                y='Points',
                color='Driver',
                title="Cumulative Points Comparison",
                labels={'Points': 'Cumulative Points', 'Race': 'Race'},
                color_discrete_map={driver1['abbreviation']: driver1['team_color'], 
                                   driver2['abbreviation']: driver2['team_color']},
                markers=True,
                height=500
            )
            
            fig.update_layout(
                xaxis_tickangle=-45,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Head-to-Head Statistics
            st.markdown("<h3>Head-to-Head Statistics</h3>", unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns(3)
            
            # Filter out rows where either driver has NaN position
            valid_races = performance_df.dropna(subset=[f"{driver1['abbreviation']} Finish", f"{driver2['abbreviation']} Finish"])
            
            # Calculate head-to-head stats
            better_finishes_d1 = sum(valid_races[f"{driver1['abbreviation']} Finish"] < valid_races[f"{driver2['abbreviation']} Finish"])
            better_finishes_d2 = sum(valid_races[f"{driver1['abbreviation']} Finish"] > valid_races[f"{driver2['abbreviation']} Finish"])
            
            total_points_d1 = performance_df[f"{driver1['abbreviation']} Points"].sum()
            total_points_d2 = performance_df[f"{driver2['abbreviation']} Points"].sum()
            
            valid_quali = performance_df.dropna(subset=[f"{driver1['abbreviation']} Grid", f"{driver2['abbreviation']} Grid"])
            better_quali_d1 = sum(valid_quali[f"{driver1['abbreviation']} Grid"] < valid_quali[f"{driver2['abbreviation']} Grid"])
            better_quali_d2 = sum(valid_quali[f"{driver1['abbreviation']} Grid"] > valid_quali[f"{driver2['abbreviation']} Grid"])
            
            with col1:
                st.markdown(f"<div class='metric-card'><div class='metric-label'>Race Head-to-Head</div><div class='metric-value'>{better_finishes_d1} - {better_finishes_d2}</div><div class='metric-label'>{driver1['abbreviation']} - {driver2['abbreviation']}</div></div>", unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"<div class='metric-card'><div class='metric-label'>Qualifying Head-to-Head</div><div class='metric-value'>{better_quali_d1} - {better_quali_d2}</div><div class='metric-label'>{driver1['abbreviation']} - {driver2['abbreviation']}</div></div>", unsafe_allow_html=True)
            
            with col3:
                st.markdown(f"<div class='metric-card'><div class='metric-label'>Total Points</div><div class='metric-value'>{total_points_d1} - {total_points_d2}</div><div class='metric-label'>{driver1['abbreviation']} - {driver2['abbreviation']}</div></div>", unsafe_allow_html=True)
        else:
            st.info("No race results available for comparison")
    else:
        st.info(f"No races found for {selected_year}")

elif page == "Telemetry Analysis":
    st.markdown("<h2 class='section-header'>Telemetry Analysis</h2>", unsafe_allow_html=True)
    
    # Get races for the selected year
    cursor.execute("""
        SELECT e.id, e.round_number, e.event_name
        FROM events e
        WHERE e.year = ?
        ORDER BY e.round_number
    """, (selected_year,))
    races = cursor.fetchall()
    
    race_options = [f"Round {race['round_number']} - {race['event_name']}" for race in races]
    
    selected_race_index = st.selectbox("Select Race", range(len(race_options)), format_func=lambda i: race_options[i])
    selected_race = races[selected_race_index]
    
    # Get all sessions for this race
    cursor.execute("""
        SELECT s.id, s.name, s.session_type
        FROM sessions s
        WHERE s.event_id = ?
        ORDER BY CASE 
                    WHEN s.session_type = 'practice' THEN 1
                    WHEN s.session_type = 'qualifying' THEN 2
                    WHEN s.session_type = 'sprint_shootout' THEN 3
                    WHEN s.session_type = 'sprint_qualifying' THEN 4
                    WHEN s.session_type = 'sprint' THEN 5
                    WHEN s.session_type = 'race' THEN 6
                    ELSE 7
                 END
    """, (selected_race['id'],))
    sessions = cursor.fetchall()
    
    session_options = [f"{session['name']} ({session['session_type']})" for session in sessions]
    
    selected_session_index = st.selectbox("Select Session", range(len(session_options)), format_func=lambda i: session_options[i])
    selected_session = sessions[selected_session_index]
    
    # Get all drivers for this session
    cursor.execute("""
        SELECT DISTINCT d.id, d.full_name, d.abbreviation, t.name as team_name, t.team_color
        FROM drivers d
        JOIN teams t ON d.team_id = t.id
        JOIN laps l ON d.id = l.driver_id
        WHERE l.session_id = ?
        ORDER BY t.name, d.full_name
    """, (selected_session['id'],))
    
    session_drivers = cursor.fetchall()
    
    if session_drivers:
        driver_options = [f"{driver['abbreviation']} - {driver['full_name']} ({driver['team_name']})" for driver in session_drivers]
        
        # Select driver and lap
        col1, col2 = st.columns(2)
        
        with col1:
            selected_driver_index = st.selectbox("Select Driver", range(len(driver_options)), format_func=lambda i: driver_options[i])
            selected_driver = session_drivers[selected_driver_index]
            
            # Get laps for selected driver
            cursor.execute("""
                SELECT l.lap_number, l.lap_time, l.is_personal_best
                FROM laps l
                WHERE l.session_id = ? AND l.driver_id = ? AND l.lap_time IS NOT NULL
                ORDER BY 
                    CASE WHEN l.is_personal_best = 1 THEN 0 ELSE 1 END,
                    l.lap_number
            """, (selected_session['id'], selected_driver['id']))
            
            driver_laps = cursor.fetchall()
            
            if driver_laps:
                lap_options = []
                for lap in driver_laps:
                    lap_time = lap['lap_time'] if lap['lap_time'] else 'No time'
                    is_best = " (Personal Best)" if lap['is_personal_best'] else ""
                    lap_options.append(f"Lap {lap['lap_number']} - {lap_time}{is_best}")
                
                selected_lap_index = st.selectbox("Select Lap", range(len(lap_options)), format_func=lambda i: lap_options[i])
                selected_lap = driver_laps[selected_lap_index]
            else:
                st.warning("No lap data available for this driver")
                selected_lap = None
        
        with col2:
            # Option to compare with another driver
            compare_option = st.checkbox("Compare with another driver")
            
            if compare_option and len(session_drivers) > 1:
                # Filter out the first selected driver
                compare_driver_options = [opt for i, opt in enumerate(driver_options) if i != selected_driver_index]
                compare_drivers = [driver for i, driver in enumerate(session_drivers) if i != selected_driver_index]
                
                compare_driver_index = st.selectbox("Select Driver to Compare", range(len(compare_driver_options)), 
                                                  format_func=lambda i: compare_driver_options[i])
                compare_driver = compare_drivers[compare_driver_index]
                
                # Get laps for comparison driver
                cursor.execute("""
                    SELECT l.lap_number, l.lap_time, l.is_personal_best
                    FROM laps l
                    WHERE l.session_id = ? AND l.driver_id = ? AND l.lap_time IS NOT NULL
                    ORDER BY 
                        CASE WHEN l.is_personal_best = 1 THEN 0 ELSE 1 END,
                        l.lap_number
                """, (selected_session['id'], compare_driver['id']))
                
                compare_laps = cursor.fetchall()
                
                if compare_laps:
                    compare_lap_options = []
                    for lap in compare_laps:
                        lap_time = lap['lap_time'] if lap['lap_time'] else 'No time'
                        is_best = " (Personal Best)" if lap['is_personal_best'] else ""
                        compare_lap_options.append(f"Lap {lap['lap_number']} - {lap_time}{is_best}")
                    
                    compare_lap_index = st.selectbox("Select Lap to Compare", range(len(compare_lap_options)), 
                                                   format_func=lambda i: compare_lap_options[i])
                    compare_lap = compare_laps[compare_lap_index]
                else:
                    st.warning("No lap data available for comparison driver")
                    compare_lap = None
            else:
                compare_driver = None
                compare_lap = None
        
        if selected_lap:
            st.markdown("<h3>Telemetry Analysis</h3>", unsafe_allow_html=True)
            
            # Get telemetry for selected lap
            cursor.execute("""
                SELECT t.session_time, t.speed, t.rpm, t.gear, t.throttle, t.brake, t.drs, t.x, t.y
                FROM telemetry t
                WHERE t.session_id = ? AND t.driver_id = ? AND t.lap_number = ?
                ORDER BY t.session_time
            """, (selected_session['id'], selected_driver['id'], selected_lap['lap_number']))
            
            telemetry_data = cursor.fetchall()
            
            if telemetry_data:
                telemetry_df = pd.DataFrame(telemetry_data)
                
                # Process session_time to get a numeric x-axis
                telemetry_df['distance'] = np.arange(len(telemetry_df))
                
                # If comparison is enabled and data exists
                compare_telemetry_df = None
                if compare_lap and compare_driver:
                    cursor.execute("""
                        SELECT t.session_time, t.speed, t.rpm, t.gear, t.throttle, t.brake, t.drs, t.x, t.y
                        FROM telemetry t
                        WHERE t.session_id = ? AND t.driver_id = ? AND t.lap_number = ?
                        ORDER BY t.session_time
                    """, (selected_session['id'], compare_driver['id'], compare_lap['lap_number']))
                    
                    compare_data = cursor.fetchall()
                    
                    if compare_data:
                        compare_telemetry_df = pd.DataFrame(compare_data)
                        compare_telemetry_df['distance'] = np.arange(len(compare_telemetry_df))
                
                # Speed Chart
                st.markdown("<h4>Speed Comparison</h4>", unsafe_allow_html=True)
                
                speed_fig = go.Figure()
                
                # Add primary driver speed
                speed_fig.add_trace(go.Scatter(
                    x=telemetry_df['distance'],
                    y=telemetry_df['speed'],
                    mode='lines',
                    name=f"{selected_driver['abbreviation']} - Lap {selected_lap['lap_number']}",
                    line=dict(color=selected_driver['team_color'], width=2)
                ))
                
                # Add comparison driver if available
                if compare_telemetry_df is not None:
                    speed_fig.add_trace(go.Scatter(
                        x=compare_telemetry_df['distance'],
                        y=compare_telemetry_df['speed'],
                        mode='lines',
                        name=f"{compare_driver['abbreviation']} - Lap {compare_lap['lap_number']}",
                        line=dict(color=compare_driver['team_color'], width=2, dash='dash')
                    ))
                
                speed_fig.update_layout(
                    title="Speed Comparison",
                    xaxis_title="Distance",
                    yaxis_title="Speed (km/h)",
                    height=500
                )
                
                st.plotly_chart(speed_fig, use_container_width=True)
                
                # Throttle/Brake Chart
                st.markdown("<h4>Throttle and Braking</h4>", unsafe_allow_html=True)
                
                # Create throttle/brake figure
                tb_fig = go.Figure()
                
                # Add throttle data
                tb_fig.add_trace(go.Scatter(
                    x=telemetry_df['distance'],
                    y=telemetry_df['throttle'],
                    mode='lines',
                    name=f"{selected_driver['abbreviation']} - Throttle",
                    line=dict(color='green', width=2)
                ))
                
                # Add brake data (convert boolean to 0-100 for visualization)
                brake_values = [100 if b else 0 for b in telemetry_df['brake']]
                tb_fig.add_trace(go.Scatter(
                    x=telemetry_df['distance'],
                    y=brake_values,
                    mode='lines',
                    name=f"{selected_driver['abbreviation']} - Brake",
                    line=dict(color='red', width=2)
                ))
                
                # Add comparison driver if available
                if compare_telemetry_df is not None:
                    tb_fig.add_trace(go.Scatter(
                        x=compare_telemetry_df['distance'],
                        y=compare_telemetry_df['throttle'],
                        mode='lines',
                        name=f"{compare_driver['abbreviation']} - Throttle",
                        line=dict(color='lightgreen', width=2, dash='dash')
                    ))
                    
                    compare_brake_values = [100 if b else 0 for b in compare_telemetry_df['brake']]
                    tb_fig.add_trace(go.Scatter(
                        x=compare_telemetry_df['distance'],
                        y=compare_brake_values,
                        mode='lines',
                        name=f"{compare_driver['abbreviation']} - Brake",
                        line=dict(color='pink', width=2, dash='dash')
                    ))
                
                tb_fig.update_layout(
                    title="Throttle and Brake Application",
                    xaxis_title="Distance",
                    yaxis_title="Value (%)",
                    height=500
                )
                
                st.plotly_chart(tb_fig, use_container_width=True)
                
                # Track Position Visualization (if x and y coordinates are available)
                if all(col in telemetry_df.columns for col in ['x', 'y']) and not telemetry_df['x'].isnull().all():
                    st.markdown("<h4>Track Position</h4>", unsafe_allow_html=True)
                    
                    track_fig = go.Figure()
                    
                    # Add primary driver track position
                    track_fig.add_trace(go.Scatter(
                        x=telemetry_df['x'],
                        y=telemetry_df['y'],
                        mode='lines',
                        name=f"{selected_driver['abbreviation']} - Lap {selected_lap['lap_number']}",
                        line=dict(color=selected_driver['team_color'], width=2)
                    ))
                    
                    # Add comparison driver if available
                    if compare_telemetry_df is not None and all(col in compare_telemetry_df.columns for col in ['x', 'y']) and not compare_telemetry_df['x'].isnull().all():
                        track_fig.add_trace(go.Scatter(
                            x=compare_telemetry_df['x'],
                            y=compare_telemetry_df['y'],
                            mode='lines',
                            name=f"{compare_driver['abbreviation']} - Lap {compare_lap['lap_number']}",
                            line=dict(color=compare_driver['team_color'], width=2, dash='dash')
                        ))
                    
                    track_fig.update_layout(
                        title="Track Position",
                        xaxis_title="X Position",
                        yaxis_title="Y Position",
                        height=600,
                        xaxis=dict(scaleanchor="y", scaleratio=1),
                        yaxis=dict(autorange="reversed")
                    )
                    
                    st.plotly_chart(track_fig, use_container_width=True)
                
                # Gear Shifts
                if 'gear' in telemetry_df.columns and not telemetry_df['gear'].isnull().all():
                    st.markdown("<h4>Gear Shifts</h4>", unsafe_allow_html=True)
                    
                    gear_fig = go.Figure()
                    
                    # Add primary driver gear
                    gear_fig.add_trace(go.Scatter(
                        x=telemetry_df['distance'],
                        y=telemetry_df['gear'],
                        mode='lines',
                        name=f"{selected_driver['abbreviation']} - Lap {selected_lap['lap_number']}",
                        line=dict(color=selected_driver['team_color'], width=2)
                    ))
                    
                    # Add comparison driver if available
                    if compare_telemetry_df is not None and 'gear' in compare_telemetry_df.columns:
                        gear_fig.add_trace(go.Scatter(
                            x=compare_telemetry_df['distance'],
                            y=compare_telemetry_df['gear'],
                            mode='lines',
                            name=f"{compare_driver['abbreviation']} - Lap {compare_lap['lap_number']}",
                            line=dict(color=compare_driver['team_color'], width=2, dash='dash')
                        ))
                    
                    gear_fig.update_layout(
                        title="Gear Shifts",
                        xaxis_title="Distance",
                        yaxis_title="Gear",
                        height=400,
                        yaxis=dict(dtick=1)
                    )
                    
                    st.plotly_chart(gear_fig, use_container_width=True)
                
                # DRS Usage
                if 'drs' in telemetry_df.columns and not telemetry_df['drs'].isnull().all():
                    st.markdown("<h4>DRS Usage</h4>", unsafe_allow_html=True)
                    
                    drs_fig = go.Figure()
                    
                    # Add primary driver DRS
                    drs_fig.add_trace(go.Scatter(
                        x=telemetry_df['distance'],
                        y=telemetry_df['drs'],
                        mode='lines',
                        name=f"{selected_driver['abbreviation']} - Lap {selected_lap['lap_number']}",
                        line=dict(color=selected_driver['team_color'], width=2)
                    ))
                    
                    # Add comparison driver if available
                    if compare_telemetry_df is not None and 'drs' in compare_telemetry_df.columns:
                        drs_fig.add_trace(go.Scatter(
                            x=compare_telemetry_df['distance'],
                            y=compare_telemetry_df['drs'],
                            mode='lines',
                            name=f"{compare_driver['abbreviation']} - Lap {compare_lap['lap_number']}",
                            line=dict(color=compare_driver['team_color'], width=2, dash='dash')
                        ))
                    
                    drs_fig.update_layout(
                        title="DRS Usage",
                        xaxis_title="Distance",
                        yaxis_title="DRS",
                        height=300,
                        yaxis=dict(dtick=1)
                    )
                    
                    st.plotly_chart(drs_fig, use_container_width=True)
            else:
                st.warning("No telemetry data available for this lap")
        else:
            st.info("Please select a lap to analyze")
    else:
        st.info("No driver data available for this session")

# Close database connection
conn.close()
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
import fastf1
import fastf1.plotting
from datetime import datetime
import numpy as np

# Set page configuration
st.set_page_config(
    page_title="F1 Data Dashboard",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configure FastF1 cache
if not os.path.exists("./fastf1_cache"):
    os.makedirs("./fastf1_cache")
fastf1.Cache.enable_cache("./fastf1_cache")

# Database connection
def get_db_connection(db_path='./f1_data.db'):
    """Create a database connection to the SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        st.error(f"Error connecting to database: {e}")
        return None

# Check if database exists
if not os.path.exists('./f1_data.db'):
    st.warning("Database not found! Please run the migration script first.")
    st.code("python migrate_sqlite.py 2023", language="bash")
    st.stop()
    
# Database is available, continue with the dashboard
conn = get_db_connection()

# Define CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 600;
        color: #ff1801;
        text-align: center;
        margin-bottom: 1rem;
    }
    .section-header {
        font-size: 1.8rem;
        font-weight: 500;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
    }
    .card {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8f9fa;
        box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
        margin-bottom: 1rem;
    }
    .metric-card {
        text-align: center;
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8f9fa;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
    }
    .metric-label {
        font-size: 1rem;
        color: #6c757d;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("<h1 class='main-header'>Formula 1 Data Dashboard</h1>", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("Navigation")
page = st.sidebar.radio("Select a page", ["Season Overview", "Race Analysis", "Driver Comparison", "Telemetry Analysis"])

# Get available years from database
cursor = conn.cursor()
cursor.execute("SELECT DISTINCT year FROM events ORDER BY year DESC")
available_years = [row[0] for row in cursor.fetchall()]

if not available_years:
    st.error("No data found in the database. Please run the migration script first.")
    st.stop()

selected_year = st.sidebar.selectbox("Select Year", available_years)

# Function to get team color mapping
def get_team_colors(year):
    """Get a mapping of teams to their colors for a specific year"""
    cursor = conn.cursor()
    cursor.execute("SELECT name, team_color FROM teams WHERE year = ?", (year,))
    return {row[0]: row[1] for row in cursor.fetchall()}

# Function to get driver abbreviations
def get_driver_abbr_mapping(year):
    """Get a mapping of driver IDs to abbreviations for a specific year"""
    cursor = conn.cursor()
    cursor.execute("SELECT id, abbreviation, full_name FROM drivers WHERE year = ?", (year,))
    return {row[0]: {"abbr": row[1], "name": row[2]} for row in cursor.fetchall()}

# Main content based on selected page
if page == "Season Overview":
    st.markdown("<h2 class='section-header'>Season Overview</h2>", unsafe_allow_html=True)
    
    # Get races for the selected year
    cursor.execute("""
        SELECT e.id, e.round_number, e.country, e.location, e.event_name, e.event_date
        FROM events e
        WHERE e.year = ?
        ORDER BY e.round_number
    """, (selected_year,))
    races = cursor.fetchall()
    
    # Display race calendar
    st.markdown("<h3>Race Calendar</h3>", unsafe_allow_html=True)
    
    race_data = []
    for race in races:
        race_date = datetime.fromisoformat(race['event_date']) if race['event_date'] else None
        race_data.append({
            'Round': race['round_number'],
            'Country': race['country'],
            'Location': race['location'],
            'Grand Prix': race['event_name'],
            'Date': race_date.strftime('%d %b %Y') if race_date else 'N/A'
        })
    
    race_df = pd.DataFrame(race_data)
    st.dataframe(race_df, use_container_width=True)
    
    # Driver Standings
    st.markdown("<h3>Driver Standings</h3>", unsafe_allow_html=True)
    
    # Get driver standings from race results
    cursor.execute("""
        SELECT d.id, d.full_name, d.abbreviation, t.name as team_name, t.team_color,
               SUM(r.points) as total_points
        FROM drivers d
        JOIN teams t ON d.team_id = t.id
        JOIN results r ON d.id = r.driver_id
        JOIN sessions s ON r.session_id = s.id
        JOIN events e ON s.event_id = e.id
        WHERE e.year = ? AND s.session_type = 'race'
        GROUP BY d.id
        ORDER BY total_points DESC
    """, (selected_year,))
    
    driver_standings = cursor.fetchall()
    
    if driver_standings:
        driver_data = []
        for i, driver in enumerate(driver_standings):
            driver_data.append({
                'Position': i + 1,
                'Driver': driver['full_name'],
                'Abbreviation': driver['abbreviation'],
                'Team': driver['team_name'],
                'Points': driver['total_points']
            })
        
        driver_df = pd.DataFrame(driver_data)
        
        fig = px.bar(
            driver_df, 
            x='Driver', 
            y='Points',
            color='Team',
            text='Points',
            title=f"{selected_year} Driver Standings",
            labels={'Points': 'Points', 'Driver': 'Driver'},
            height=500
        )
        
        #