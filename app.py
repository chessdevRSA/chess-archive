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
from utils.db_manager import DatabaseManager

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
if 'job_ids' not in st.session_state:
    st.session_state.job_ids = []
if 'db_manager' not in st.session_state:
    st.session_state.db_manager = DatabaseManager()
    # Initialize database and storage structure
    create_storage_structure()

# Main title and description
st.title("Chess Game Archiver")
st.markdown("""
This application collects and archives chess games from online platforms 
like Chess.com and Lichess based on player information.
""")

# Create tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Player Database", 
    "Game Collection",
    "Scheduling",
    "Archive Statistics",
    "Settings"
])

# Player Database tab
with tab1:
    st.header("Player Database")
    
    # Initialize player data from database
    if st.session_state.player_data is None:
        # Try to load from database
        db_player_data = st.session_state.db_manager.get_player_data()
        if not db_player_data.empty:
            st.session_state.player_data = db_player_data
    
    # File upload section
    st.subheader("Import Players")
    
    uploaded_file = st.file_uploader("Upload player data CSV file", type="csv")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            is_valid, message = validate_player_data(df)
            
            if is_valid:
                # Import to database
                success = st.session_state.db_manager.import_player_data(df)
                
                if success:
                    st.success(f"Successfully imported {len(df)} players.")
                    # Update session state with latest player data
                    st.session_state.player_data = st.session_state.db_manager.get_player_data()
                else:
                    st.error("Error importing player data to database.")
            else:
                st.error(message)
        except Exception as e:
            st.error(f"Error reading CSV file: {str(e)}")
    
    # Display player data
    st.subheader("Player List")
    
    if st.session_state.player_data is not None:
        # Display data with edit capability
        edited_df = st.data_editor(
            st.session_state.player_data,
            hide_index=True,
            num_rows="dynamic",
            key="player_editor"
        )
        
        # Save button for edited data
        if st.button("Save Changes"):
            # Update database with edited data
            success = st.session_state.db_manager.import_player_data(edited_df)
            
            if success:
                st.success("Changes saved successfully.")
                # Update session state with latest player data
                st.session_state.player_data = st.session_state.db_manager.get_player_data()
            else:
                st.error("Error saving changes.")
    else:
        st.info("No player data available. Please import a CSV file.")
        
        # Show template format button
        if st.button("Download Template CSV"):
            template_df = pd.DataFrame({
                'fide_id': ['12345678', '87654321'],
                'name': ['Magnus Carlsen', 'Hikaru Nakamura'],
                'rating': [2850, 2750],
                'title': ['GM', 'GM'],
                'federation': ['NOR', 'USA'],
                'birth_year': [1990, 1987],
                'chesscom_username': ['MagnusCarlsen', 'Hikaru'],
                'lichess_username': ['DrNykterstein', 'Hikaru']
            })
            
            # Convert DataFrame to CSV and create a download link
            csv = template_df.to_csv(index=False)
            st.download_button(
                label="Download Template CSV",
                data=csv,
                file_name="chess_players_template.csv",
                mime="text/csv",
            )

# Game Collection tab
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
            
            # Time controls section
            st.subheader("Time Controls")
            
            # Global time controls (for all players)
            time_control_mode = st.radio(
                "Time Control Selection Mode",
                ["All Time Controls", "Select Specific Time Controls", "Per Player"],
                index=0
            )
            
            global_time_controls = []
            per_player_time_controls = {}
            
            if time_control_mode == "Select Specific Time Controls":
                global_time_controls = st.multiselect(
                    "Select time controls for all players",
                    ["bullet", "blitz", "rapid", "classical", "correspondence", "other"],
                    []
                )
            elif time_control_mode == "Per Player":
                st.info("Configure time controls for each selected player below:")
                
                for player in selected_players:
                    per_player_time_controls[player] = st.multiselect(
                        f"Time controls for {player}",
                        ["bullet", "blitz", "rapid", "classical", "correspondence", "other"],
                        []
                    )
            
        with col2:
            time_period = st.radio(
                "Time period",
                ["Last month", "Last 3 months", "Last 6 months", "Last year", "All available"],
                index=0
            )
            
            # Changed to 0 to represent unlimited as per requirements
            max_games = st.number_input(
                "Maximum games per player (0 for unlimited)",
                min_value=0,
                max_value=100000,
                value=0,
                help="Set to 0 to download all available games"
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
                    
                    # Get time controls for this player
                    if time_control_mode == "All Time Controls":
                        player_time_controls = None
                    elif time_control_mode == "Select Specific Time Controls":
                        player_time_controls = global_time_controls
                    else:  # Per Player
                        player_time_controls = per_player_time_controls.get(player, [])
                    
                    # Update progress
                    st.session_state.scraping_progress[player]["status"] = "in_progress"
                    
                    # Initialize API clients
                    if "Chess.com" in platforms and not pd.isna(chesscom_username):
                        try:
                            chess_com_client = ChessComClient()
                            games = chess_com_client.get_player_games(
                                chesscom_username, 
                                time_period,
                                max_games,
                                player_time_controls
                            )
                            
                            # Process and save games
                            is_active = len(games) > 0
                            
                            if games:
                                processed_games = process_pgn_data(games, 'chess.com', player, fide_id)
                                save_pgn_files(processed_games, 'chess.com', player, fide_id, is_active)
                                st.session_state.scraping_progress[player]["chess_com_games"] = len(processed_games)
                            else:
                                # Handle inactive account
                                save_pgn_files([], 'chess.com', player, fide_id, is_active)
                                st.session_state.scraping_progress[player]["chess_com_games"] = 0
                            
                            # Log collection
                            st.session_state.db_manager.log_collection(
                                fide_id,
                                'chess.com',
                                time_period,
                                len(games) if games else 0,
                                player_time_controls,
                                "success" if is_active else "inactive"
                            )
                            
                        except Exception as e:
                            st.session_state.scraping_progress[player]["chess_com_error"] = str(e)
                            
                            # Log error
                            st.session_state.db_manager.log_collection(
                                fide_id,
                                'chess.com',
                                time_period,
                                0,
                                player_time_controls,
                                "error",
                                str(e)
                            )
                    
                    if "Lichess" in platforms and not pd.isna(lichess_username):
                        try:
                            lichess_client = LichessClient()
                            games = lichess_client.get_player_games(
                                lichess_username, 
                                time_period,
                                max_games,
                                player_time_controls
                            )
                            
                            # Process and save games
                            is_active = len(games) > 0
                            
                            if games:
                                processed_games = process_pgn_data(games, 'lichess', player, fide_id)
                                save_pgn_files(processed_games, 'lichess', player, fide_id, is_active)
                                st.session_state.scraping_progress[player]["lichess_games"] = len(processed_games)
                            else:
                                # Handle inactive account
                                save_pgn_files([], 'lichess', player, fide_id, is_active)
                                st.session_state.scraping_progress[player]["lichess_games"] = 0
                            
                            # Log collection
                            st.session_state.db_manager.log_collection(
                                fide_id,
                                'lichess',
                                time_period,
                                len(games) if games else 0,
                                player_time_controls,
                                "success" if is_active else "inactive"
                            )
                            
                        except Exception as e:
                            st.session_state.scraping_progress[player]["lichess_error"] = str(e)
                            
                            # Log error
                            st.session_state.db_manager.log_collection(
                                fide_id,
                                'lichess',
                                time_period,
                                0,
                                player_time_controls,
                                "error",
                                str(e)
                            )
                    
                    st.session_state.scraping_progress[player]["status"] = "completed"
                
                st.session_state.scraping_running = False
        
        # Display scraping progress
        if st.session_state.scraping_progress:
            st.subheader("Collection Progress")
            
            for player, progress in st.session_state.scraping_progress.items():
                status = progress["status"]
                
                if status == "pending":
                    st.info(f"{player}: Pending")
                elif status == "in_progress":
                    st.info(f"{player}: In progress...")
                elif status == "completed":
                    success_msg = f"{player}: Completed"
                    
                    if "chess_com_games" in progress:
                        success_msg += f" - Chess.com: {progress['chess_com_games']} games"
                    
                    if "lichess_games" in progress:
                        success_msg += f" - Lichess: {progress['lichess_games']} games"
                    
                    st.success(success_msg)
                    
                    # Display errors if any
                    if "chess_com_error" in progress:
                        st.error(f"Chess.com error: {progress['chess_com_error']}")
                    
                    if "lichess_error" in progress:
                        st.error(f"Lichess error: {progress['lichess_error']}")
                    
        # Display inactive accounts
        st.subheader("Inactive Accounts")
        inactive_df = st.session_state.db_manager.get_inactive_accounts()
        
        if not inactive_df.empty:
            st.write(f"The following {len(inactive_df)} accounts have been marked as inactive (no recent games found):")
            st.dataframe(inactive_df)
        else:
            st.info("No inactive accounts detected.")

# Scheduling tab
with tab3:
    st.header("Scheduled Collections")
    
    if st.session_state.player_data is None:
        st.warning("Please import player data first in the Player Database tab.")
    else:
        st.subheader("Schedule Monthly Collection")
        
        col1, col2 = st.columns(2)
        
        with col1:
            scheduled_players = st.multiselect(
                "Select players for scheduled collection",
                st.session_state.player_data['name'].tolist(),
                []
            )
            
            scheduled_platforms = st.multiselect(
                "Platforms to collect from",
                ["Chess.com", "Lichess"],
                ["Chess.com", "Lichess"]
            )
            
            # Time controls for scheduled collections
            scheduled_time_controls = st.multiselect(
                "Time controls to collect",
                ["bullet", "blitz", "rapid", "classical", "correspondence", "other"],
                []
            )
            
        with col2:
            day_of_month = st.slider(
                "Day of month to run collection",
                1, 28, 1
            )
            
            hour_of_day = st.slider(
                "Hour of day to run collection (24h format)",
                0, 23, 0
            )
            
            scheduled_max_games = st.number_input(
                "Maximum games per collection (0 for unlimited)",
                min_value=0,
                max_value=100000,
                value=0,
                help="Set to 0 to download all available games"
            )
        
        if st.button("Schedule Collection"):
            if len(scheduled_players) == 0:
                st.error("Please select at least one player.")
            else:
                job_ids = schedule_scraping_tasks(
                    st.session_state.scheduler,
                    scheduled_players,
                    st.session_state.player_data,
                    scheduled_platforms,
                    day_of_month,
                    hour_of_day,
                    scheduled_time_controls,
                    scheduled_max_games
                )
                
                st.session_state.job_ids.extend(job_ids)
                st.success(f"Successfully scheduled collection for {len(job_ids)} players.")
        
        # Display scheduled tasks
        st.subheader("Scheduled Tasks")
        
        scheduled_tasks_df = st.session_state.db_manager.get_scheduled_tasks()
        
        if not scheduled_tasks_df.empty:
            # Convert JSON columns to readable format
            scheduled_tasks_df['platforms_str'] = scheduled_tasks_df['platforms'].apply(lambda x: ', '.join(x))
            
            # Format time controls for display
            def format_time_controls(tc_list):
                if tc_list and len(tc_list) > 0:
                    return ', '.join(tc_list)
                return "All"
                
            scheduled_tasks_df['time_controls_str'] = scheduled_tasks_df['time_controls'].apply(format_time_controls)
            
            # Display columns of interest
            display_df = scheduled_tasks_df[[
                'player_name', 'fide_id', 'platforms_str', 'time_controls_str',
                'day_of_month', 'hour', 'max_games', 'is_active'
            ]].copy()
            
            display_df.columns = [
                'Player', 'FIDE ID', 'Platforms', 'Time Controls',
                'Day of Month', 'Hour', 'Max Games', 'Active'
            ]
            
            st.dataframe(display_df)
            
            # Add delete buttons
            if not scheduled_tasks_df.empty:
                task_to_delete = st.selectbox(
                    "Select a task to delete",
                    scheduled_tasks_df['player_name'].tolist()
                )
                
                if st.button("Delete Selected Task"):
                    task_row = scheduled_tasks_df[scheduled_tasks_df['player_name'] == task_to_delete].iloc[0]
                    job_id = task_row['job_id']
                    
                    # Remove from scheduler
                    st.session_state.scheduler.remove_job(job_id)
                    
                    # Remove from database
                    success = st.session_state.db_manager.delete_scheduled_task(job_id)
                    
                    if success:
                        st.success(f"Successfully deleted scheduled task for {task_to_delete}.")
                        # Remove from session state
                        if job_id in st.session_state.job_ids:
                            st.session_state.job_ids.remove(job_id)
                    else:
                        st.error(f"Error deleting scheduled task for {task_to_delete}.")
        else:
            st.info("No scheduled tasks available.")

# Archive Statistics tab
with tab4:
    st.header("Archive Statistics")
    
    # Get archive statistics
    stats = get_archive_stats()
    
    # Display statistics
    if stats:
        display_collection_stats(stats)
        
        # Inactive Accounts
        st.subheader("Inactive Account Details")
        inactive_df = st.session_state.db_manager.get_inactive_accounts()
        
        if not inactive_df.empty:
            st.dataframe(inactive_df)
        else:
            st.info("No inactive accounts detected.")
            
        # Collection History
        st.subheader("Recent Collection History")
        # Use the database collection logs
        collection_stats = st.session_state.db_manager.get_collection_stats()
        
        if collection_stats and "recent_collections" in collection_stats:
            recent_df = pd.DataFrame(collection_stats["recent_collections"])
            if not recent_df.empty:
                st.dataframe(recent_df)
            else:
                st.info("No collection history available.")
        else:
            st.info("No collection history available.")
    else:
        st.info("No archived data available yet.")

# Settings tab
with tab5:
    st.header("Settings")
    
    st.subheader("Database Operations")
    
    # Database backup and restore
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Backup Database"):
            try:
                from shutil import copyfile
                backup_path = "data/chess_archive_backup.db"
                copyfile("data/chess_archive.db", backup_path)
                st.success(f"Database backup created at {backup_path}")
            except Exception as e:
                st.error(f"Error creating database backup: {str(e)}")
    
    with col2:
        if st.button("Restore Database from Backup"):
            try:
                backup_path = "data/chess_archive_backup.db"
                if os.path.exists(backup_path):
                    from shutil import copyfile
                    copyfile(backup_path, "data/chess_archive.db")
                    st.success("Database restored from backup.")
                    # Reload player data
                    st.session_state.player_data = st.session_state.db_manager.get_player_data()
                else:
                    st.error("Backup file not found.")
            except Exception as e:
                st.error(f"Error restoring database: {str(e)}")
    
    # Display app version and info
    st.subheader("About")
    st.markdown("""
    **Chess Game Archiver** v1.0
    
    A tool for archiving chess games from multiple platforms based on player FIDE information.
    
    Features:
    - Import FIDE player data
    - Collect games from Chess.com and Lichess
    - Filter by time controls
    - Track inactive accounts
    - Schedule automatic monthly collections
    - Maintain archive statistics
    """)
