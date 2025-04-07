import requests
import time
import datetime
import io
import chess.pgn
import os
from typing import List, Dict, Any, Optional, Union

class ChessComClient:
    """Client for interacting with the Chess.com API"""
    
    # Define standard time control categories for Chess.com
    TIME_CONTROL_MAPPING = {
        "bullet": ["bullet"],
        "blitz": ["blitz"],
        "rapid": ["rapid"],
        "classical": ["daily", "standard"],
        "other": ["chess960", "bughouse", "kingofthehill", "threecheck", "crazyhouse"]
    }
    
    def __init__(self, request_delay: float = 1.0):
        """
        Initialize the Chess.com API client
        
        Args:
            request_delay: Time in seconds to wait between API requests
        """
        self.base_url = "https://api.chess.com/pub"
        self.request_delay = request_delay
        
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a request to the Chess.com API with rate limiting
        
        Args:
            endpoint: API endpoint to call
            params: Optional query parameters
            
        Returns:
            JSON response from the API
        """
        url = f"{self.base_url}/{endpoint}"
        time.sleep(self.request_delay)  # Rate limiting
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if response.status_code == 429:
                # Rate limit exceeded
                retry_after = int(response.headers.get('Retry-After', 60))
                print(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                return self._make_request(endpoint, params)
            else:
                raise Exception(f"Chess.com API error: {str(e)}")
    
    def _is_matching_time_control(self, pgn: str, time_controls: List[str]) -> bool:
        """
        Check if a PGN game matches the requested time controls
        
        Args:
            pgn: PGN string of the game
            time_controls: List of time control categories to match
            
        Returns:
            True if game matches one of the requested time controls, False otherwise
        """
        if not time_controls:
            return True
            
        # Extract TimeControl tag from PGN
        time_control_line = None
        for line in pgn.split('\n'):
            if '[TimeControl' in line:
                time_control_line = line
                break
                
        if not time_control_line:
            return False
            
        # Extract Event tag to help identify the game type
        event_line = None
        for line in pgn.split('\n'):
            if '[Event' in line:
                event_line = line
                break
                
        # Determine game type based on TimeControl and Event tags
        game_type = None
        
        # Check event line for common time control identifiers
        if event_line:
            event_lower = event_line.lower()
            if "bullet" in event_lower:
                game_type = "bullet"
            elif "blitz" in event_lower:
                game_type = "blitz"
            elif "rapid" in event_lower:
                game_type = "rapid"
            elif "classical" in event_lower or "standard" in event_lower:
                game_type = "classical"
            elif "daily" in event_lower:
                game_type = "classical"
                
        # If we couldn't determine from Event, check TimeControl
        if not game_type and time_control_line:
            # Extract time value (format: [TimeControl "300"])
            time_value = time_control_line.split('"')[1]
            
            try:
                # Handle different TimeControl formats
                if "+" in time_value:  # Format like "300+2"
                    base_time = int(time_value.split('+')[0])
                elif "/" in time_value:  # Format like "1/259200"
                    # This is likely a daily game
                    game_type = "classical"
                else:
                    base_time = int(time_value)
                    
                if not game_type:
                    # Categorize based on base time
                    if base_time < 180:  # Less than 3 minutes
                        game_type = "bullet"
                    elif base_time < 600:  # Less than 10 minutes
                        game_type = "blitz"
                    elif base_time < 1800:  # Less than 30 minutes
                        game_type = "rapid"
                    else:
                        game_type = "classical"
            except:
                # If we can't parse the time control, default to None
                pass
                
        # Check if determined game type matches requested time controls
        if game_type:
            for requested in time_controls:
                if requested == game_type:
                    return True
                    
                # Check in mapping table for alternative names
                if requested in self.TIME_CONTROL_MAPPING and game_type in self.TIME_CONTROL_MAPPING[requested]:
                    return True
                    
        return False
    
    def get_player_games(
        self, 
        username: str, 
        time_period: str = "Last month", 
        max_games: int = 0,
        time_controls: List[str] = None
    ) -> List[str]:
        """
        Get a player's games from Chess.com
        
        Args:
            username: Chess.com username
            time_period: Time period to fetch games for
            max_games: Maximum number of games to fetch (0 for unlimited)
            time_controls: List of time controls to filter by (e.g., ["rapid", "blitz"])
                           None or empty list means all time controls
            
        Returns:
            List of games in PGN format
        """
        # Calculate date range based on time period
        end_date = datetime.datetime.now()
        
        if time_period == "Last month":
            start_date = end_date - datetime.timedelta(days=30)
        elif time_period == "Last 3 months":
            start_date = end_date - datetime.timedelta(days=90)
        elif time_period == "Last 6 months":
            start_date = end_date - datetime.timedelta(days=180)
        elif time_period == "Last year":
            start_date = end_date - datetime.timedelta(days=365)
        else:  # All available
            # Chess.com API only provides archives by month, so get all available
            try:
                archives_data = self._make_request(f"player/{username}/games/archives")
                archives = archives_data.get('archives', [])
            except Exception as e:
                print(f"Error getting archives for {username}: {str(e)}")
                return []
                
            # Fetch games from each archive
            all_games = []
            for archive_url in archives:
                try:
                    month_data = requests.get(archive_url).json()
                    games = month_data.get('games', [])
                    
                    for game in games:
                        if 'pgn' in game:
                            pgn = game['pgn']
                            # Apply time control filter
                            if self._is_matching_time_control(pgn, time_controls):
                                all_games.append(pgn)
                            
                    time.sleep(self.request_delay)
                except Exception as e:
                    print(f"Error fetching games from archive {archive_url}: {str(e)}")
                    continue
                    
            return all_games
        
        # Get archives within date range
        try:
            archives_data = self._make_request(f"player/{username}/games/archives")
            archives = archives_data.get('archives', [])
        except Exception as e:
            print(f"Error getting archives for {username}: {str(e)}")
            return []
        
        # Filter archives within date range
        start_year_month = f"{start_date.year:04d}/{start_date.month:02d}"
        end_year_month = f"{end_date.year:04d}/{end_date.month:02d}"
        
        filtered_archives = [
            archive for archive in archives 
            if archive.split("/")[-2:][0] >= start_year_month and archive.split("/")[-2:][0] <= end_year_month
        ]
        
        # Fetch games from each filtered archive
        all_games = []
        for archive_url in filtered_archives:
            try:
                month_data = requests.get(archive_url).json()
                games = month_data.get('games', [])
                
                for game in games:
                    # Filter games by date if needed
                    if 'pgn' in game:
                        pgn = game['pgn']
                        # Apply time control filter
                        if self._is_matching_time_control(pgn, time_controls):
                            all_games.append(pgn)
                        
                time.sleep(self.request_delay)
            except Exception as e:
                print(f"Error fetching games from archive {archive_url}: {str(e)}")
                continue
                
        return all_games

class LichessClient:
    """Client for interacting with the Lichess API"""
    
    # Define standard time control categories for Lichess
    TIME_CONTROL_MAPPING = {
        "bullet": ["bullet"],
        "blitz": ["blitz"],
        "rapid": ["rapid"],
        "classical": ["classical"],
        "correspondence": ["correspondence"],
        "other": ["ultraBullet", "crazyhouse", "chess960", "kingOfTheHill", "threeCheck", "antichess", "atomic", "horde", "racingKings"]
    }
    
    def __init__(self, request_delay: float = 1.0):
        """
        Initialize the Lichess API client
        
        Args:
            request_delay: Time in seconds to wait between API requests
        """
        self.base_url = "https://lichess.org/api"
        self.request_delay = request_delay
        
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """
        Make a request to the Lichess API with rate limiting
        
        Args:
            endpoint: API endpoint to call
            params: Optional query parameters
            
        Returns:
            Response object from the API
        """
        url = f"{self.base_url}/{endpoint}"
        time.sleep(self.request_delay)  # Rate limiting
        
        try:
            response = requests.get(url, params=params, stream=True)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if response.status_code == 429:
                # Rate limit exceeded
                retry_after = int(response.headers.get('Retry-After', 60))
                print(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                return self._make_request(endpoint, params)
            else:
                raise Exception(f"Lichess API error: {str(e)}")
    
    def get_player_games(
        self, 
        username: str, 
        time_period: str = "Last month", 
        max_games: int = 0,
        time_controls: List[str] = None
    ) -> List[str]:
        """
        Get a player's games from Lichess
        
        Args:
            username: Lichess username
            time_period: Time period to fetch games for
            max_games: Maximum number of games to fetch (0 for unlimited)
            time_controls: List of time controls to filter by (e.g., ["rapid", "blitz"])
                           None or empty list means all time controls
            
        Returns:
            List of games in PGN format
        """
        # Calculate date range based on time period
        end_date = datetime.datetime.now()
        
        if time_period == "Last month":
            since = int((end_date - datetime.timedelta(days=30)).timestamp() * 1000)
        elif time_period == "Last 3 months":
            since = int((end_date - datetime.timedelta(days=90)).timestamp() * 1000)
        elif time_period == "Last 6 months":
            since = int((end_date - datetime.timedelta(days=180)).timestamp() * 1000)
        elif time_period == "Last year":
            since = int((end_date - datetime.timedelta(days=365)).timestamp() * 1000)
        else:  # All available
            since = None
        
        params = {
            "pgnInJson": "false",
            "clocks": "false",
            "evals": "false",
            "opening": "true",
        }
        
        if since:
            params["since"] = since
            
        if max_games > 0:
            params["max"] = max_games
            
        # Add time control filters for Lichess
        if time_controls:
            # Lichess uses specific perf parameters for time controls
            lichess_perfs = []
            for tc in time_controls:
                if tc == "bullet":
                    lichess_perfs.append("bullet")
                elif tc == "blitz":
                    lichess_perfs.append("blitz")
                elif tc == "rapid":
                    lichess_perfs.append("rapid")
                elif tc == "classical":
                    lichess_perfs.append("classical")
                elif tc == "correspondence":
                    lichess_perfs.append("correspondence")
                    
            if lichess_perfs:
                params["perfType"] = ",".join(lichess_perfs)
        
        try:
            response = self._make_request(f"games/user/{username}", params)
            pgn_text = response.text
            
            # Parse PGN text into a list of games
            games = []
            pgn_io = io.StringIO(pgn_text)
            
            while True:
                game = chess.pgn.read_game(pgn_io)
                if game is None:
                    break
                
                # Convert game to PGN string
                exporter = chess.pgn.StringExporter()
                pgn_string = game.accept(exporter)
                games.append(pgn_string)
            
            return games
        except Exception as e:
            print(f"Error fetching Lichess games for {username}: {str(e)}")
            return []
