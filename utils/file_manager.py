import os
import json
import datetime
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from pathlib import Path
import io
import chess.pgn

def create_storage_structure():
    """Create the storage directory structure if it doesn't exist"""
    base_dir = "data"
    
    # Create base directories
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        
    players_dir = os.path.join(base_dir, "players")
    if not os.path.exists(players_dir):
        os.makedirs(players_dir)
        
    logs_dir = os.path.join(base_dir, "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
        
    return base_dir

def get_player_directory(platform: str, player_name: str, fide_id: str) -> str:
    """
    Get the directory path for a player's games
    
    Args:
        platform: 'chess.com' or 'lichess'
        player_name: Name of the player
        fide_id: FIDE ID of the player
        
    Returns:
        Path to the player's directory
    """
    base_dir = "data"
    player_dir = os.path.join(base_dir, "players", fide_id)
    
    if not os.path.exists(player_dir):
        os.makedirs(player_dir)
        
        # Create a player info JSON file
        player_info = {
            "fide_id": fide_id,
            "name": player_name,
            "platforms": {}
        }
        
        with open(os.path.join(player_dir, "player_info.json"), "w") as f:
            json.dump(player_info, f)
    
    # Create platform directory if it doesn't exist
    platform_dir = os.path.join(player_dir, platform)
    if not os.path.exists(platform_dir):
        os.makedirs(platform_dir)
        
    return platform_dir

def save_pgn_files(
    pgn_list: List[str], 
    platform: str, 
    player_name: str, 
    fide_id: str,
    is_active: bool = True
) -> int:
    """
    Save PGN games to files organized by year and month
    
    Args:
        pgn_list: List of PGN strings
        platform: 'chess.com' or 'lichess'
        player_name: Name of the player
        fide_id: FIDE ID of the player
        is_active: Whether the account is active
        
    Returns:
        Number of games saved
    """
    if not pgn_list and is_active:
        # No games and account is active - nothing to save
        return 0
        
    platform_dir = get_player_directory(platform, player_name, fide_id)
    games_by_date = {}
    
    # If no games but account marked as inactive, we keep the existing files
    if not pgn_list and not is_active:
        return 0  # No new games saved
    
    # Organize games by date
    for pgn_str in pgn_list:
        try:
            pgn = io.StringIO(pgn_str)
            game = chess.pgn.read_game(pgn)
            
            if game is None:
                continue
                
            date_str = game.headers.get("Date", "").replace(".", "-")
            
            # If date is incomplete, use today's date
            if not date_str or "?" in date_str:
                today = datetime.datetime.now()
                date_str = f"{today.year}.{today.month:02d}.{today.day:02d}"
            
            try:
                parts = date_str.split(".")
                if len(parts) >= 2:
                    year = parts[0]
                    month = parts[1]
                    
                    year_month = f"{year}-{month}"
                    if year_month not in games_by_date:
                        games_by_date[year_month] = []
                        
                    games_by_date[year_month].append(pgn_str)
                else:
                    # If date format is invalid, use current year/month
                    today = datetime.datetime.now()
                    year_month = f"{today.year}-{today.month:02d}"
                    if year_month not in games_by_date:
                        games_by_date[year_month] = []
                        
                    games_by_date[year_month].append(pgn_str)
            except Exception as e:
                print(f"Error parsing date {date_str}: {str(e)}")
                # Use current year/month as fallback
                today = datetime.datetime.now()
                year_month = f"{today.year}-{today.month:02d}"
                if year_month not in games_by_date:
                    games_by_date[year_month] = []
                    
                games_by_date[year_month].append(pgn_str)
                
        except Exception as e:
            print(f"Error processing game: {str(e)}")
            continue
    
    # Save games by year and month
    total_saved = 0
    for year_month, games in games_by_date.items():
        try:
            year, month = year_month.split("-")
            year_dir = os.path.join(platform_dir, year)
            
            if not os.path.exists(year_dir):
                os.makedirs(year_dir)
                
            filename = os.path.join(year_dir, f"{year}-{month}.pgn")
            
            # For active accounts, we replace the content
            # For inactive accounts, we would skip this step
            with open(filename, "w") as f:
                for game in games:
                    f.write(game)
                    f.write("\n\n")
                    total_saved += 1
                    
        except Exception as e:
            print(f"Error saving games for {year_month}: {str(e)}")
            continue
    
    # Update player_info.json with platform info and active status
    player_info_path = os.path.join("data", "players", fide_id, "player_info.json")
    
    try:
        with open(player_info_path, "r") as f:
            player_info = json.load(f)
            
        if "platforms" not in player_info:
            player_info["platforms"] = {}
            
        if platform not in player_info["platforms"]:
            player_info["platforms"][platform] = {
                "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_games": total_saved,
                "is_active": is_active
            }
        else:
            player_info["platforms"][platform]["last_update"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            player_info["platforms"][platform]["is_active"] = is_active
            
            # Only update total games if we have new games or the account is active
            if is_active or total_saved > 0:
                player_info["platforms"][platform]["total_games"] = player_info["platforms"][platform].get("total_games", 0) + total_saved
            
        with open(player_info_path, "w") as f:
            json.dump(player_info, f)
            
    except Exception as e:
        print(f"Error updating player info: {str(e)}")
    
    return total_saved

def get_archive_stats() -> Dict[str, Any]:
    """
    Get statistics about the archived games
    
    Returns:
        Dictionary with archive statistics
    """
    base_dir = "data"
    players_dir = os.path.join(base_dir, "players")
    
    if not os.path.exists(players_dir):
        return {}
        
    # Get list of all players
    players = []
    for fide_id in os.listdir(players_dir):
        player_dir = os.path.join(players_dir, fide_id)
        
        if not os.path.isdir(player_dir):
            continue
            
        player_info_path = os.path.join(player_dir, "player_info.json")
        
        if os.path.exists(player_info_path):
            try:
                with open(player_info_path, "r") as f:
                    player_info = json.load(f)
                    players.append(player_info)
            except Exception as e:
                print(f"Error reading player info: {str(e)}")
                continue
    
    # Calculate statistics
    total_players = len(players)
    total_games = 0
    games_by_platform = {"chess.com": 0, "lichess": 0}
    games_by_year = {}
    
    # Keep track of active and inactive accounts
    active_accounts = {"chess.com": 0, "lichess": 0}
    inactive_accounts = {"chess.com": 0, "lichess": 0}
    
    for player in players:
        platforms = player.get("platforms", {})
        
        for platform, platform_info in platforms.items():
            platform_games = platform_info.get("total_games", 0)
            total_games += platform_games
            
            # Count active/inactive accounts
            if platform_info.get("is_active", True):
                active_accounts[platform] = active_accounts.get(platform, 0) + 1
            else:
                inactive_accounts[platform] = inactive_accounts.get(platform, 0) + 1
            
            if platform in games_by_platform:
                games_by_platform[platform] += platform_games
                
            # Count games by year
            player_dir = os.path.join(players_dir, player["fide_id"], platform)
            
            if os.path.exists(player_dir):
                for year in os.listdir(player_dir):
                    year_dir = os.path.join(player_dir, year)
                    
                    if os.path.isdir(year_dir) and year.isdigit():
                        if year not in games_by_year:
                            games_by_year[year] = 0
                            
                        # Count games in PGN files
                        for pgn_file in os.listdir(year_dir):
                            if pgn_file.endswith(".pgn"):
                                pgn_path = os.path.join(year_dir, pgn_file)
                                
                                try:
                                    # Count games in PGN file
                                    with open(pgn_path, "r") as f:
                                        content = f.read()
                                        # Rough count based on Result tags
                                        game_count = content.count('[Result "')
                                        games_by_year[year] += game_count
                                except Exception as e:
                                    print(f"Error counting games in {pgn_path}: {str(e)}")
                                    continue
    
    # Prepare statistics
    stats = {
        "total_players": total_players,
        "total_games": total_games,
        "games_by_platform": games_by_platform,
        "games_by_year": games_by_year,
        "active_accounts": active_accounts,
        "inactive_accounts": inactive_accounts,
        "players": players
    }
    
    return stats
