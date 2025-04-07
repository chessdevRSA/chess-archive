import requests
import time
import datetime
import io
import chess.pgn
import os
from typing import List, Dict, Any, Optional, Union

class ChessComClient:
    """Client for interacting with the Chess.com API"""
    
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
    
    def get_player_games(
        self, 
        username: str, 
        time_period: str = "Last month", 
        max_games: int = 100
    ) -> List[str]:
        """
        Get a player's games from Chess.com
        
        Args:
            username: Chess.com username
            time_period: Time period to fetch games for
            max_games: Maximum number of games to fetch (0 for unlimited)
            
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
                            all_games.append(game['pgn'])
                            
                        if max_games > 0 and len(all_games) >= max_games:
                            return all_games
                            
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
                        all_games.append(game['pgn'])
                        
                    if max_games > 0 and len(all_games) >= max_games:
                        return all_games
                        
                time.sleep(self.request_delay)
            except Exception as e:
                print(f"Error fetching games from archive {archive_url}: {str(e)}")
                continue
                
        return all_games

class LichessClient:
    """Client for interacting with the Lichess API"""
    
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
        max_games: int = 100
    ) -> List[str]:
        """
        Get a player's games from Lichess
        
        Args:
            username: Lichess username
            time_period: Time period to fetch games for
            max_games: Maximum number of games to fetch (0 for unlimited)
            
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
