import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# File: frontend/streamlit_dashboard.py

import streamlit as st
import pandas as pd
import sqlite3
from backend.config import SQLITE_DB_PATH
import plotly.express as px

# Utility to get DB connection
def get_connection():
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

######################
# PAGE 1: Events & Sessions
######################
def page_events_sessions():
    st.title("F1 Data Dashboard: Events & Sessions")

    conn = get_connection()
    # List available years
    years_df = pd.read_sql_query("SELECT DISTINCT year FROM events ORDER BY year DESC", conn)
    year = st.selectbox("Select Year", years_df["year"].tolist())

    # Query events for selected year
    events_df = pd.read_sql_query(
        "SELECT id, round_number, event_name, event_date FROM events WHERE year = ? ORDER BY round_number",
        conn, params=(year,)
    )
    st.write("## Events")
    st.dataframe(events_df)

    # Choose an event
    event_row = st.selectbox("Choose Event", events_df["event_name"].tolist())
    if event_row:
        # Get event_id
        sel_event = events_df[events_df["event_name"] == event_row].iloc[0]
        event_id = sel_event["id"]

        # Show sessions
        sessions_df = pd.read_sql_query(
            "SELECT * FROM sessions WHERE event_id = ? ORDER BY session_type",
            conn, params=(event_id,)
        )
        st.write("## Sessions")
        st.dataframe(sessions_df)

    conn.close()

######################
# PAGE 2: Results
######################
def page_results():
    st.title("Session Results")
    conn = get_connection()

    # Choose year & event
    years_df = pd.read_sql_query("SELECT DISTINCT year FROM events ORDER BY year DESC", conn)
    year = st.selectbox("Select Year", years_df["year"].tolist(), key="res_year")

    events_df = pd.read_sql_query(
        "SELECT id, round_number, event_name FROM events WHERE year = ? ORDER BY round_number",
        conn, params=(year,)
    )
    event_row = st.selectbox("Select Event", events_df["event_name"].tolist(), key="res_event")
    if event_row:
        sel_event = events_df[events_df["event_name"] == event_row].iloc[0]
        event_id = sel_event["id"]

        # sessions
        sessions_df = pd.read_sql_query(
            "SELECT id, name, session_type FROM sessions WHERE event_id = ? ORDER BY session_type",
            conn, params=(event_id,)
        )
        sess_row = st.selectbox("Select Session", sessions_df["name"].tolist(), key="res_session")
        if sess_row:
            sel_sess = sessions_df[sessions_df["name"] == sess_row].iloc[0]
            session_id = sel_sess["id"]

            # Now query results
            res_df = pd.read_sql_query("""
                SELECT r.position, r.classified_position, r.grid_position, r.status, r.points,
                       d.full_name as driver_name, d.abbreviation, t.name as team_name
                FROM results r
                JOIN drivers d ON r.driver_id = d.id
                JOIN teams t ON d.team_id = t.id
                WHERE r.session_id = ?
                ORDER BY r.position
            """, conn, params=(session_id,))
            st.write("## Session Results")
            st.dataframe(res_df)

    conn.close()

######################
# PAGE 3: Lap Times
######################
def page_lap_times():
    st.title("Lap Times")
    conn = get_connection()

    # Choose year & event
    years_df = pd.read_sql_query("SELECT DISTINCT year FROM events ORDER BY year DESC", conn)
    year = st.selectbox("Select Year", years_df["year"].tolist(), key="laps_year")

    events_df = pd.read_sql_query(
        "SELECT id, round_number, event_name FROM events WHERE year = ? ORDER BY round_number",
        conn, params=(year,)
    )
    event_row = st.selectbox("Select Event", events_df["event_name"].tolist(), key="laps_event")
    if event_row:
        sel_event = events_df[events_df["event_name"] == event_row].iloc[0]
        event_id = sel_event["id"]

        # sessions
        sessions_df = pd.read_sql_query(
            "SELECT id, name, session_type FROM sessions WHERE event_id = ? ORDER BY session_type",
            conn, params=(event_id,)
        )
        sess_row = st.selectbox("Select Session", sessions_df["name"].tolist(), key="laps_session")
        if sess_row:
            sel_sess = sessions_df[sessions_df["name"] == sess_row].iloc[0]
            session_id = sel_sess["id"]

            # Query laps
            laps_df = pd.read_sql_query("""
                SELECT l.lap_number, l.lap_time, l.is_personal_best, l.compound,
                       d.abbreviation as driver, t.team_color
                FROM laps l
                JOIN drivers d ON l.driver_id = d.id
                JOIN teams t ON d.team_id = t.id
                WHERE l.session_id = ?
                ORDER BY d.abbreviation, l.lap_number
            """, conn, params=(session_id,))

            st.write("## Lap Times")
            st.dataframe(laps_df)

            # Possibly visualize best laps
            if not laps_df.empty:
                # Convert lap_time from "0 days 00:01:30.123000" to seconds
                def to_seconds(ts):
                    if ts and "days" in ts:
                        parts = ts.split()
                        # e.g. "0 days 00:01:30.123000"
                        if len(parts) >= 3:
                            hms = parts[2]
                            hh, mm, ss = hms.split(":")
                            return int(hh)*3600 + int(mm)*60 + float(ss)
                    return None
                laps_df["lap_time_s"] = laps_df["lap_time"].apply(to_seconds)

                fig = px.scatter(
                    laps_df,
                    x="lap_number",
                    y="lap_time_s",
                    color="driver",
                    hover_data=["compound", "is_personal_best"]
                )
                st.plotly_chart(fig, use_container_width=True)

    conn.close()

######################
# PAGE 4: Telemetry
######################
def page_telemetry():
    st.title("Telemetry Comparison")
    conn = get_connection()

    # Choose year & event
    years_df = pd.read_sql_query("SELECT DISTINCT year FROM events ORDER BY year DESC", conn)
    year = st.selectbox("Select Year", years_df["year"].tolist(), key="tel_year")

    events_df = pd.read_sql_query(
        "SELECT id, round_number, event_name FROM events WHERE year = ? ORDER BY round_number",
        conn, params=(year,)
    )
    event_row = st.selectbox("Select Event", events_df["event_name"].tolist(), key="tel_event")
    if event_row:
        sel_event = events_df[events_df["event_name"] == event_row].iloc[0]
        event_id = sel_event["id"]

        # sessions
        sessions_df = pd.read_sql_query(
            "SELECT id, name, session_type FROM sessions WHERE event_id = ? ORDER BY session_type",
            conn, params=(event_id,)
        )
        sess_row = st.selectbox("Select Session", sessions_df["name"].tolist(), key="tel_session")
        if sess_row:
            sel_sess = sessions_df[sessions_df["name"] == sess_row].iloc[0]
            session_id = sel_sess["id"]

            # pick driver
            drivers_df = pd.read_sql_query("""
                SELECT DISTINCT d.abbreviation, d.full_name
                FROM drivers d
                JOIN laps l ON d.id = l.driver_id
                WHERE l.session_id = ?
            """, conn, params=(session_id,))
            driver_abbr = st.selectbox("Select Driver", drivers_df["abbreviation"].tolist(), key="tel_driver")

            # pick lap
            if driver_abbr:
                laps_for_driver = pd.read_sql_query("""
                    SELECT l.lap_number
                    FROM laps l
                    JOIN drivers d ON l.driver_id = d.id
                    WHERE l.session_id = ?
                      AND d.abbreviation = ?
                    ORDER BY l.lap_number
                """, conn, params=(session_id, driver_abbr))
                lap_choice = st.selectbox("Select Lap", laps_for_driver["lap_number"].tolist(), key="tel_lap")

                if lap_choice:
                    # Query telemetry
                    tel_df = pd.read_sql_query("""
                        SELECT speed, rpm, gear, throttle, brake, drs, time, session_time
                        FROM telemetry
                        WHERE session_id = ?
                          AND lap_number = ?
                          AND driver_id = (
                            SELECT d.id FROM drivers d
                            WHERE d.abbreviation = ? AND d.year = ?
                          )
                        ORDER BY id
                    """, conn, params=(session_id, lap_choice, driver_abbr, year))

                    st.write("## Telemetry Data")
                    st.dataframe(tel_df)

                    # Plot Speed vs. time
                    if not tel_df.empty:
                        fig = px.line(tel_df, y="speed", title="Speed vs. Sample")
                        st.plotly_chart(fig, use_container_width=True)

    conn.close()

######################
# Main
######################
def main():
    st.sidebar.title("F1 Dashboard")
    pages = {
        "Events & Sessions": page_events_sessions,
        "Session Results": page_results,
        "Lap Times": page_lap_times,
        "Telemetry": page_telemetry
    }
    choice = st.sidebar.radio("Go to", list(pages.keys()))
    pages[choice]()

if __name__ == "__main__":
    main()
