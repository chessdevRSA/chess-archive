import pandas as pd
import io
import chess.pgn
from typing import List, Dict, Tuple, Any, Optional, Union

def validate_player_data(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Validate the player data DataFrame to ensure it has the required columns
    
    Args:
        df: DataFrame containing player data
        
    Returns:
        Tuple of (is_valid, message)
    """
    required_columns = ['fide_id', 'name']
    
    # Check for required columns
    for col in required_columns:
        if col not in df.columns:
            return False, f"Missing required column: {col}"
    
    # Check for at least one username column
    if 'chesscom_username' not in df.columns and 'lichess_username' not in df.columns:
        return False, "DataFrame must contain at least one of 'chesscom_username' or 'lichess_username'"
    
    # Check for duplicate FIDE IDs
    if df['fide_id'].duplicated().any():
        return False, "DataFrame contains duplicate FIDE IDs"
    
    # Ensure FIDE IDs are strings
    df['fide_id'] = df['fide_id'].astype(str)
    
    return True, "Validation successful"

def process_pgn_data(
    pgn_list: List[str], 
    platform: str, 
    player_name: str, 
    fide_id: str
) -> List[str]:
    """
    Process a list of PGN games to add or correct metadata
    
    Args:
        pgn_list: List of PGN strings
        platform: 'chess.com' or 'lichess'
        player_name: Name of the player
        fide_id: FIDE ID of the player
        
    Returns:
        List of processed PGN strings
    """
    processed_games = []
    
    for pgn_str in pgn_list:
        try:
            # Parse the PGN
            pgn = io.StringIO(pgn_str)
            game = chess.pgn.read_game(pgn)
            
            if game is None:
                continue
            
            # Add or update headers
            headers = game.headers
            
            # Add source platform
            headers["Source"] = platform
            
            # Add FIDE ID for the player if we can determine which side they played
            white_player = headers.get("White", "")
            black_player = headers.get("Black", "")
            
            if platform == "chess.com":
                # For Chess.com, try to match by name
                if player_name in white_player:
                    headers["WhiteFideId"] = fide_id
                elif player_name in black_player:
                    headers["BlackFideId"] = fide_id
            elif platform == "lichess":
                # For Lichess, usernames are usually exact
                if player_name in white_player:
                    headers["WhiteFideId"] = fide_id
                elif player_name in black_player:
                    headers["BlackFideId"] = fide_id
            
            # Convert back to PGN string
            exporter = chess.pgn.StringExporter()
            processed_pgn = game.accept(exporter)
            processed_games.append(processed_pgn)
            
        except Exception as e:
            print(f"Error processing PGN game: {str(e)}")
            continue
    
    return processed_games

def extract_game_metadata(pgn_str: str) -> Dict[str, Any]:
    """
    Extract metadata from a PGN game
    
    Args:
        pgn_str: PGN string
        
    Returns:
        Dictionary of game metadata
    """
    try:
        pgn = io.StringIO(pgn_str)
        game = chess.pgn.read_game(pgn)
        
        if game is None:
            return {}
        
        # Extract basic metadata
        headers = game.headers
        
        metadata = {
            "event": headers.get("Event", ""),
            "date": headers.get("Date", ""),
            "white": headers.get("White", ""),
            "black": headers.get("Black", ""),
            "result": headers.get("Result", ""),
            "white_elo": headers.get("WhiteElo", ""),
            "black_elo": headers.get("BlackElo", ""),
            "time_control": headers.get("TimeControl", ""),
            "termination": headers.get("Termination", ""),
            "source": headers.get("Source", ""),
            "white_fide_id": headers.get("WhiteFideId", ""),
            "black_fide_id": headers.get("BlackFideId", ""),
        }
        
        return metadata
    except Exception as e:
        print(f"Error extracting metadata: {str(e)}")
        return {}
