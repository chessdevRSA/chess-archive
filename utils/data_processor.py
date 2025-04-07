import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
import io
import chess.pgn
import datetime

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
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return False, f"Missing required columns: {', '.join(missing_columns)}"
    
    # Check if there are any rows
    if len(df) == 0:
        return False, "DataFrame is empty"
    
    # Check if fide_id is unique
    if df['fide_id'].duplicated().any():
        return False, "FIDE IDs must be unique"
    
    return True, "DataFrame is valid"

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
            # Parse PGN
            pgn_io = io.StringIO(pgn_str)
            game = chess.pgn.read_game(pgn_io)
            
            if game is None:
                continue
            
            # Add or update headers
            game.headers["FideId"] = fide_id
            
            # Add custom headers for tracking
            game.headers["ArchiverSource"] = platform
            game.headers["ArchiverTimestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Export game back to PGN
            exporter = chess.pgn.StringExporter()
            processed_pgn = game.accept(exporter)
            
            processed_games.append(processed_pgn)
        except Exception as e:
            print(f"Error processing game: {str(e)}")
            # Add the original game if there was an error
            processed_games.append(pgn_str)
    
    return processed_games

def extract_game_metadata(pgn_str: str) -> Dict[str, Any]:
    """
    Extract metadata from a PGN game
    
    Args:
        pgn_str: PGN string
        
    Returns:
        Dictionary of game metadata
    """
    metadata = {}
    
    try:
        # Parse PGN
        pgn_io = io.StringIO(pgn_str)
        game = chess.pgn.read_game(pgn_io)
        
        if game is None:
            return metadata
        
        # Extract headers
        for key, value in game.headers.items():
            metadata[key] = value
        
        # Add additional metadata
        metadata['moves_count'] = sum(1 for _ in game.mainline_moves())
        
        # Get the outcome
        if "Result" in metadata:
            result = metadata["Result"]
            if result == "1-0":
                metadata['outcome'] = "white_win"
            elif result == "0-1":
                metadata['outcome'] = "black_win"
            elif result == "1/2-1/2":
                metadata['outcome'] = "draw"
            else:
                metadata['outcome'] = "unknown"
    except Exception as e:
        print(f"Error extracting metadata: {str(e)}")
    
    return metadata
