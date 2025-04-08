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
        
        # Add user agent to avoid 403 errors
        headers = {
            'User-Agent': 'Chess Game Archiver/1.0 (https://replit.com; for educational purposes)',
            'Accept': 'application/json',
        }
        
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response') and e.response.status_code == 429:
                # Rate limit exceeded
                retry_after = int(e.response.headers.get('Retry-After', 60))
                print(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                return self._make_request(endpoint, params)
            elif hasattr(e, 'response') and e.response.status_code == 404:
                # Handle 404 - User not found or no games
                print(f"No games found at {url}")
                return {"archives": []}
            elif hasattr(e, 'response') and e.response.status_code == 403:
                # Handle 403 - API access forbidden
                print(f"Access forbidden by Chess.com API at {url}. Possibly temporary IP restriction.")
                # Return empty response to continue execution
                return {"archives": []}
            else:
                raise Exception(f"Chess.com API error: {str(e)}")
        except Exception as e:
            print(f"Unexpected error accessing Chess.com API: {str(e)}")
            # Return empty response to continue execution
            return {"archives": []}
    
    def _categorize_time_control(self, time_control: str) -> str:
        """
        Categorize a time control into a standard category
        
        Args:
            time_control: Time control string (e.g., "600+0")
            
        Returns:
            Category name ("bullet", "blitz", "rapid", "daily")
        """
        # For correspondence games
        if time_control == "-":
            return "daily"
        
        # Parse time control format (base time + increment)
        try:
            parts = time_control.split("+")
            if len(parts) >= 1:
                base_time = int(parts[0])
                
                # Categorize based on base time in seconds (from the HTML sample)
                if base_time < 180:  # Less than 3 minutes
                    return "bullet"
                elif base_time < 600:  # 3-10 minutes
                    return "blitz"
                elif base_time < 3600:  # 10-60 minutes
                    return "rapid"
                else:  # More than 60 minutes
                    return "daily"
        except (ValueError, IndexError):
            # If we can't parse the time control, check for special formats
            if "/" in time_control:  # Format like "1/259200" (daily)
                return "daily"
        
        return "other"
    
    def _is_matching_time_control(self, pgn: str, time_controls: Optional[List[str]]) -> bool:
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
        time_control_match = None
        for line in pgn.split('\n'):
            if '[TimeControl' in line:
                # Extract the value inside quotes
                try:
                    time_control_match = line.split('"')[1]
                    break
                except IndexError:
                    continue
                
        if not time_control_match:
            # Check for variants - skip them
            for line in pgn.split('\n'):
                if '[Variant' in line:
                    return False
            # If we couldn't find time control info, default to not matching
            return False
            
        # Get category for this time control
        category = self._categorize_time_control(time_control_match)
        
        # Check if the category matches any of the requested time controls
        return category in time_controls
    
    def _fetch_games_for_month(self, username: str, year: int, month: int, time_controls: Optional[List[str]]) -> List[str]:
        """
        Fetch games for a specific month from Chess.com
        
        Args:
            username: Chess.com username
            year: Year to fetch
            month: Month to fetch
            time_controls: List of time control categories to include
            
        Returns:
            List of PGN strings
        """
        url = f"{self.base_url}/player/{username}/games/{year}/{str(month).zfill(2)}"
        
        headers = {
            'User-Agent': 'Chess Game Archiver/1.0 (https://replit.com; for educational purposes)',
            'Accept': 'application/json',
        }
        
        try:
            time.sleep(self.request_delay)  # Rate limiting
            response = requests.get(url, headers=headers)
            
            if not response.ok:
                return []
                
            data = response.json()
            if not data.get('games'):
                return []
            
            filtered_games = []
            for game in data['games']:
                if "pgn" not in game:
                    continue
                    
                pgn = game['pgn']
                
                # Skip chess variants
                variant_found = False
                for line in pgn.split('\n'):
                    if '[Variant' in line:
                        variant_found = True
                        break
                if variant_found:
                    continue
                
                # Apply time control filter
                if time_controls and not self._is_matching_time_control(pgn, time_controls):
                    continue
                
                filtered_games.append(pgn)
            
            return filtered_games
            
        except Exception as e:
            print(f"Error fetching games for {username} ({year}/{month}): {str(e)}")
            return []

    def get_player_games(
        self, 
        username: str, 
        time_period: str = "Last month", 
        max_games: int = 0,
        time_controls: Optional[List[str]] = None
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
        # Get user information
        try:
            user_info = self._make_request(f"player/{username}")
            if not user_info:
                print(f"User {username} not found on Chess.com")
                return []
        except Exception as e:
            print(f"Error getting user info for {username}: {str(e)}")
            return []
            
        # Determine date range based on time period
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
            # Use account creation date
            joined_timestamp = user_info.get('joined', 0)
            start_date = datetime.datetime.fromtimestamp(joined_timestamp)
                
        # Collect all months between start and end date
        all_months = []
        start_year, start_month = start_date.year, start_date.month
        end_year, end_month = end_date.year, end_date.month
        
        current_year, current_month = start_year, start_month
        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            all_months.append((current_year, current_month))
            
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
        
        # Fetch games for each month
        all_games = []
        
        for year, month in all_months:
            # Fetch and filter games for this month
            month_games = self._fetch_games_for_month(
                username=username,
                year=year,
                month=month,
                time_controls=time_controls
            )
            
            all_games.extend(month_games)
            
            # Check if we've reached the maximum number of games
            if max_games > 0 and len(all_games) >= max_games:
                return all_games[:max_games]
                
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
        
        # Add user agent to avoid 403 errors
        headers = {
            'User-Agent': 'Chess Game Archiver/1.0 (https://replit.com; for educational purposes)',
            'Accept': 'application/x-chess-pgn',
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, stream=True)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response') and e.response.status_code == 429:
                # Rate limit exceeded
                retry_after = int(e.response.headers.get('Retry-After', 60))
                print(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                return self._make_request(endpoint, params)
            elif hasattr(e, 'response') and e.response.status_code == 404:
                # Handle 404 - User not found or no games
                print(f"No games or user not found at {url}")
                # Return a mock response object with empty text
                mock_response = requests.Response()
                mock_response.status_code = 404
                mock_response._content = b''  # Empty bytes content
                return mock_response
            elif hasattr(e, 'response') and e.response.status_code == 403:
                # Handle 403 - API access forbidden
                print(f"Access forbidden by Lichess API at {url}. Possibly temporary IP restriction.")
                # Return a mock response object with empty text
                mock_response = requests.Response()
                mock_response.status_code = 403
                mock_response._content = b''  # Empty bytes content
                return mock_response
            else:
                print(f"HTTP error from Lichess API: {str(e)}")
                # Return a mock response object with empty text
                mock_response = requests.Response()
                mock_response.status_code = e.response.status_code if hasattr(e, 'response') else 500
                mock_response._content = b''  # Empty bytes content
                return mock_response
        except Exception as e:
            print(f"Unexpected error accessing Lichess API: {str(e)}")
            # Return a mock response object with empty text
            mock_response = requests.Response()
            mock_response.status_code = 500
            mock_response._content = b''  # Empty bytes content
            return mock_response
    
    def get_player_games(
        self, 
        username: str, 
        time_period: str = "Last month", 
        max_games: int = 0,
        time_controls: Optional[List[str]] = None
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
            params["since"] = str(since)
            
        if max_games > 0:
            params["max"] = str(max_games)
            
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
