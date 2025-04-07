import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime
import matplotlib.pyplot as plt
import plotly.express as px
from apscheduler.schedulers.background import BackgroundScheduler
import chess.pgn

from utils.api_clients import ChessComClient, LichessClient
from utils.data_processor import process_pgn_data, validate_player_data
from utils.file_manager import save_pgn_files, create_storage_structure, get_archive_stats
from utils.scheduler import schedule_scraping_tasks, get_scheduled_tasks
from utils.visualizers import display_collection_stats

# Set page configuration
st.set_page_config(
    page_title="Chess Game Archiver",
    page_icon="♟️",
    layout="wide",
)

# Initialize session state variables if they don't exist
if 'player_data' not in st.session_state:
    st.session_state.player_data = None
if 'scraping_running' not in st.session_state:
    st.session_state.scraping_running = False
if 'scraping_progress' not in st.session_state:
    st.session_state.scraping_progress = {}
if 'scheduler' not in st.session_state:
    st.session_state.scheduler = BackgroundScheduler()
    st.session_state.scheduler.start()
if 'scheduled_tasks' not in st.session_state:
    st.session_state.scheduled_tasks = []
if 'archive_stats' not in st.session_state:
    st.session_state.archive_stats = None
if 'error_log' not in st.session_state:
    st.session_state.error_log = []

# Create base storage directories if they don't exist
data_dir = "data"
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
    os.makedirs(os.path.join(data_dir, "players"))
    os.makedirs(os.path.join(data_dir, "logs"))

# Title and description
st.title("Chess Game Archiver")
st.markdown(
    "A tool for scraping and archiving chess games from Chess.com and Lichess based on FIDE player information."
)

# Main navigation
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Player Database", "Game Collection", "Scheduling", 
    "Archive Statistics", "Settings"
])

# Tab 1: Player Database Management
with tab1:
    st.header("Player Database Management")
    
    st.subheader("Import FIDE Player Data")
    uploaded_file = st.file_uploader(
        "Upload CSV or Excel file with FIDE player data", 
        type=["csv", "xlsx"]
    )
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### Required Columns")
        st.info("""
        - `fide_id`: FIDE ID number
        - `name`: Player's full name
        - `chesscom_username`: Chess.com username (optional)
        - `lichess_username`: Lichess username (optional)
        """)
    
    with col2:
        st.markdown("### Sample Format")
        sample_data = {
            "fide_id": ["12345678", "87654321"],
            "name": ["Magnus Carlsen", "Hikaru Nakamura"],
            "chesscom_username": ["MagnusCarlsen", "Hikaru"],
            "lichess_username": ["DrNykterstein", "Hikaru"]
        }
        st.dataframe(pd.DataFrame(sample_data))
    
    if uploaded_file is not None:
        try:
            # Determine file type and read accordingly
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            # Validate data
            validation_result, validation_message = validate_player_data(df)
            
            if validation_result:
                st.session_state.player_data = df
                st.success(f"Successfully imported {len(df)} player records!")
            else:
                st.error(validation_message)
        except Exception as e:
            st.error(f"Error importing file: {str(e)}")
    
    if st.session_state.player_data is not None:
        st.subheader("Player Database Preview")
        st.dataframe(st.session_state.player_data)
        
        st.subheader("Database Statistics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_players = len(st.session_state.player_data)
            st.metric("Total Players", total_players)
        
        with col2:
            chesscom_count = st.session_state.player_data['chesscom_username'].count()
            st.metric("Chess.com Usernames", 
                     f"{chesscom_count} ({round(chesscom_count/total_players*100)}%)")
        
        with col3:
            lichess_count = st.session_state.player_data['lichess_username'].count()
            st.metric("Lichess Usernames", 
                     f"{lichess_count} ({round(lichess_count/total_players*100)}%)")

# Tab 2: Game Collection
with tab2:
    st.header("Chess Game Collection")
    
    if st.session_state.player_data is None:
        st.warning("Please import player data first in the Player Database tab.")
    else:
        st.subheader("Manual Collection")
        
        col1, col2 = st.columns(2)
        
        with col1:
            platforms = st.multiselect(
                "Platforms to collect from",
                ["Chess.com", "Lichess"],
                ["Chess.com", "Lichess"]
            )
            
            selected_players = st.multiselect(
                "Select players to collect games for",
                st.session_state.player_data['name'].tolist(),
                []
            )
            
        with col2:
            time_period = st.radio(
                "Time period",
                ["Last month", "Last 3 months", "Last 6 months", "Last year", "All available"],
                index=0
            )
            
            max_games = st.number_input(
                "Maximum games per player (0 for unlimited)",
                min_value=0,
                max_value=10000,
                value=100
            )
        
        if st.button("Start Collection", disabled=st.session_state.scraping_running):
            if len(selected_players) == 0:
                st.error("Please select at least one player.")
            else:
                st.session_state.scraping_running = True
                st.session_state.scraping_progress = {player: {"status": "pending", "progress": 0} 
                                                     for player in selected_players}
                
                # Create storage structure for the collected games
                create_storage_structure()
                
                # Start collection process in the background
                for player in selected_players:
                    player_data = st.session_state.player_data[
                        st.session_state.player_data['name'] == player
                    ].iloc[0]
                    
                    fide_id = player_data['fide_id']
                    chesscom_username = player_data.get('chesscom_username')
                    lichess_username = player_data.get('lichess_username')
                    
                    # Update progress
                    st.session_state.scraping_progress[player]["status"] = "in_progress"
                    
                    # Initialize API clients
                    if "Chess.com" in platforms and not pd.isna(chesscom_username):
                        try:
                            chess_com_client = ChessComClient()
                            games = chess_com_client.get_player_games(
                                chesscom_username, 
                                time_period,
                                max_games
                            )
                            
                            # Process and save games
                            if games:
                                processed_games = process_pgn_data(games, 'chess.com', player, fide_id)
                                save_pgn_files(processed_games, 'chess.com', player, fide_id)
                                st.session_state.scraping_progress[player]["chess_com_games"] = len(games)
                        except Exception as e:
                            error_msg = f"Error collecting Chess.com games for {player}: {str(e)}"
                            st.session_state.error_log.append({
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "player": player,
                                "platform": "Chess.com",
                                "error": str(e)
                            })
                    
                    if "Lichess" in platforms and not pd.isna(lichess_username):
                        try:
                            lichess_client = LichessClient()
                            games = lichess_client.get_player_games(
                                lichess_username,
                                time_period,
                                max_games
                            )
                            
                            # Process and save games
                            if games:
                                processed_games = process_pgn_data(games, 'lichess', player, fide_id)
                                save_pgn_files(processed_games, 'lichess', player, fide_id)
                                st.session_state.scraping_progress[player]["lichess_games"] = len(games)
                        except Exception as e:
                            error_msg = f"Error collecting Lichess games for {player}: {str(e)}"
                            st.session_state.error_log.append({
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "player": player,
                                "platform": "Lichess",
                                "error": str(e)
                            })
                    
                    # Update progress
                    st.session_state.scraping_progress[player]["status"] = "completed"
                    st.session_state.scraping_progress[player]["progress"] = 100
                
                st.session_state.scraping_running = False
                st.session_state.archive_stats = get_archive_stats()
                st.rerun()
        
        # Show progress
        if st.session_state.scraping_running:
            st.subheader("Collection Progress")
            for player, progress in st.session_state.scraping_progress.items():
                if progress["status"] != "pending":
                    st.progress(progress["progress"])
                    st.text(f"{player}: {progress['status']}")
        
        # Show collection results if available
        if any(prog["status"] == "completed" for player, prog in st.session_state.scraping_progress.items()):
            st.subheader("Collection Results")
            results = []
            
            for player, progress in st.session_state.scraping_progress.items():
                if progress["status"] == "completed":
                    player_result = {
                        "Player": player,
                        "Chess.com Games": progress.get("chess_com_games", 0),
                        "Lichess Games": progress.get("lichess_games", 0),
                        "Total Games": progress.get("chess_com_games", 0) + progress.get("lichess_games", 0)
                    }
                    results.append(player_result)
            
            st.table(pd.DataFrame(results))
        
        # Error log
        if st.session_state.error_log:
            with st.expander("View Error Log"):
                st.table(pd.DataFrame(st.session_state.error_log))

# Tab 3: Scheduling
with tab3:
    st.header("Scheduled Collection")
    
    if st.session_state.player_data is None:
        st.warning("Please import player data first in the Player Database tab.")
    else:
        st.subheader("Set Up Monthly Collection")
        
        col1, col2 = st.columns(2)
        
        with col1:
            schedule_platforms = st.multiselect(
                "Platforms to collect from",
                ["Chess.com", "Lichess"],
                ["Chess.com", "Lichess"],
                key="schedule_platforms"
            )
            
            schedule_all_players = st.checkbox("Schedule for all players in database", value=False)
            
            if not schedule_all_players:
                schedule_players = st.multiselect(
                    "Select players to schedule collection for",
                    st.session_state.player_data['name'].tolist(),
                    []
                )
            else:
                schedule_players = st.session_state.player_data['name'].tolist()
            
        with col2:
            collection_day = st.number_input(
                "Day of month to collect (1-28)",
                min_value=1,
                max_value=28,
                value=1
            )
            
            collection_hour = st.number_input(
                "Hour to collect (0-23)",
                min_value=0,
                max_value=23,
                value=0
            )
            
            max_games_monthly = st.number_input(
                "Maximum games per player per month (0 for unlimited)",
                min_value=0,
                max_value=10000,
                value=1000,
                key="max_games_monthly"
            )
        
        if st.button("Schedule Monthly Collection"):
            if len(schedule_players) == 0:
                st.error("Please select at least one player.")
            else:
                # Schedule the scraping tasks
                try:
                    new_tasks = schedule_scraping_tasks(
                        st.session_state.scheduler,
                        schedule_players,
                        st.session_state.player_data,
                        schedule_platforms,
                        collection_day,
                        collection_hour,
                        max_games_monthly
                    )
                    
                    st.session_state.scheduled_tasks.extend(new_tasks)
                    st.success(f"Successfully scheduled collection for {len(schedule_players)} players!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error scheduling collection: {str(e)}")
        
        # Display scheduled tasks
        st.subheader("Scheduled Tasks")
        
        if not st.session_state.scheduled_tasks:
            st.info("No scheduled tasks. Set up a collection schedule above.")
        else:
            scheduled_tasks = get_scheduled_tasks(st.session_state.scheduler, st.session_state.scheduled_tasks)
            scheduled_df = pd.DataFrame(scheduled_tasks)
            
            if not scheduled_df.empty:
                st.dataframe(scheduled_df)
                
                if st.button("Clear All Scheduled Tasks"):
                    for job_id in st.session_state.scheduled_tasks:
                        try:
                            st.session_state.scheduler.remove_job(job_id)
                        except:
                            pass
                    st.session_state.scheduled_tasks = []
                    st.success("All scheduled tasks have been cleared.")
                    st.rerun()
            else:
                st.info("No active scheduled tasks.")

# Tab 4: Archive Statistics
with tab4:
    st.header("Archive Statistics")
    
    if st.button("Refresh Archive Statistics"):
        st.session_state.archive_stats = get_archive_stats()
        st.success("Archive statistics updated!")
    
    if st.session_state.archive_stats is None:
        st.session_state.archive_stats = get_archive_stats()
    
    if st.session_state.archive_stats:
        display_collection_stats(st.session_state.archive_stats)
    else:
        st.info("No games have been collected yet. Use the Game Collection tab to start collecting games.")

# Tab 5: Settings
with tab5:
    st.header("Settings")
    
    # API Rate Limiting Settings
    st.subheader("API Rate Limiting")
    
    col1, col2 = st.columns(2)
    
    with col1:
        chesscom_delay = st.number_input(
            "Chess.com API delay between requests (seconds)",
            min_value=0.5,
            max_value=10.0,
            value=1.0,
            step=0.1
        )
    
    with col2:
        lichess_delay = st.number_input(
            "Lichess API delay between requests (seconds)",
            min_value=0.5,
            max_value=10.0, 
            value=1.0,
            step=0.1
        )
    
    if st.button("Save Settings"):
        # Update settings in a mock configuration file
        settings = {
            "chesscom_delay": chesscom_delay,
            "lichess_delay": lichess_delay
        }
        
        try:
            # In a real app, save to a config file
            with open(os.path.join(data_dir, "settings.json"), "w") as f:
                import json
                json.dump(settings, f)
            st.success("Settings saved successfully!")
        except Exception as e:
            st.error(f"Error saving settings: {str(e)}")
    
    # Error Log
    st.subheader("Error Log")
    
    if st.session_state.error_log:
        error_df = pd.DataFrame(st.session_state.error_log)
        st.dataframe(error_df)
        
        if st.button("Clear Error Log"):
            st.session_state.error_log = []
            st.success("Error log cleared!")
            st.rerun()
    else:
        st.info("No errors have been logged.")
