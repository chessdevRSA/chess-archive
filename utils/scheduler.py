import datetime
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.job import Job

from utils.api_clients import ChessComClient, LichessClient
from utils.data_processor import process_pgn_data
from utils.file_manager import save_pgn_files

def schedule_scraping_task(
    scheduler: BackgroundScheduler,
    player_name: str,
    fide_id: str,
    chesscom_username: Optional[str],
    lichess_username: Optional[str],
    platforms: List[str],
    day_of_month: int,
    hour: int,
    max_games: int = 1000
) -> str:
    """
    Schedule a monthly game collection task for a player
    
    Args:
        scheduler: APScheduler instance
        player_name: Name of the player
        fide_id: FIDE ID of the player
        chesscom_username: Chess.com username
        lichess_username: Lichess username
        platforms: List of platforms to collect from
        day_of_month: Day of the month to run the collection
        hour: Hour of the day to run the collection
        max_games: Maximum games to collect per platform
        
    Returns:
        Job ID of the scheduled task
    """
    def collection_task():
        # Collection function that runs on schedule
        print(f"Running scheduled collection for {player_name} ({fide_id})")
        
        # Chess.com collection
        if "Chess.com" in platforms and chesscom_username and not pd.isna(chesscom_username):
            try:
                # Get games from Chess.com for the last month
                chess_com_client = ChessComClient()
                games = chess_com_client.get_player_games(
                    chesscom_username,
                    "Last month",
                    max_games
                )
                
                # Process and save games
                if games:
                    processed_games = process_pgn_data(games, 'chess.com', player_name, fide_id)
                    save_pgn_files(processed_games, 'chess.com', player_name, fide_id)
                    print(f"Saved {len(processed_games)} Chess.com games for {player_name}")
            except Exception as e:
                print(f"Error collecting Chess.com games for {player_name}: {str(e)}")
        
        # Lichess collection
        if "Lichess" in platforms and lichess_username and not pd.isna(lichess_username):
            try:
                # Get games from Lichess for the last month
                lichess_client = LichessClient()
                games = lichess_client.get_player_games(
                    lichess_username,
                    "Last month",
                    max_games
                )
                
                # Process and save games
                if games:
                    processed_games = process_pgn_data(games, 'lichess', player_name, fide_id)
                    save_pgn_files(processed_games, 'lichess', player_name, fide_id)
                    print(f"Saved {len(processed_games)} Lichess games for {player_name}")
            except Exception as e:
                print(f"Error collecting Lichess games for {player_name}: {str(e)}")
    
    # Schedule the task to run monthly
    job = scheduler.add_job(
        collection_task,
        'cron',
        day=day_of_month,
        hour=hour,
        minute=0,
        id=f"collection_{fide_id}",
        replace_existing=True,
        misfire_grace_time=3600  # 1 hour grace time for misfires
    )
    
    return job.id

def schedule_scraping_tasks(
    scheduler: BackgroundScheduler,
    player_names: List[str],
    player_data: pd.DataFrame,
    platforms: List[str],
    day_of_month: int,
    hour: int,
    max_games: int = 1000
) -> List[str]:
    """
    Schedule monthly game collection tasks for multiple players
    
    Args:
        scheduler: APScheduler instance
        player_names: List of player names to schedule
        player_data: DataFrame with player information
        platforms: List of platforms to collect from
        day_of_month: Day of the month to run the collection
        hour: Hour of the day to run the collection
        max_games: Maximum games to collect per platform
        
    Returns:
        List of job IDs for the scheduled tasks
    """
    job_ids = []
    
    for player_name in player_names:
        # Get player data
        player_info = player_data[player_data['name'] == player_name]
        
        if len(player_info) == 0:
            print(f"Player {player_name} not found in database")
            continue
            
        player_row = player_info.iloc[0]
        fide_id = player_row['fide_id']
        chesscom_username = player_row.get('chesscom_username')
        lichess_username = player_row.get('lichess_username')
        
        # Schedule the task
        job_id = schedule_scraping_task(
            scheduler,
            player_name,
            fide_id,
            chesscom_username,
            lichess_username,
            platforms,
            day_of_month,
            hour,
            max_games
        )
        
        job_ids.append(job_id)
    
    return job_ids

def get_scheduled_tasks(
    scheduler: BackgroundScheduler,
    job_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Get information about scheduled tasks
    
    Args:
        scheduler: APScheduler instance
        job_ids: List of job IDs to get information for
        
    Returns:
        List of dictionaries with task information
    """
    tasks = []
    
    for job_id in job_ids:
        job = scheduler.get_job(job_id)
        
        if job is not None:
            # Extract player FIDE ID from job ID
            fide_id = job_id.replace("collection_", "")
            
            # Get next run time
            next_run = job.next_run_time
            
            if next_run:
                next_run_str = next_run.strftime("%Y-%m-%d %H:%M:%S")
            else:
                next_run_str = "Not scheduled"
            
            task_info = {
                "job_id": job_id,
                "fide_id": fide_id,
                "next_run": next_run_str,
                "status": "Active" if job.next_run_time else "Paused"
            }
            
            tasks.append(task_info)
    
    return tasks
