import os
import sqlite3
import json
import datetime
from typing import List, Dict, Any, Optional, Union
import pandas as pd

class DatabaseManager:
    """Manager for handling SQLite database operations"""
    
    def __init__(self, db_path: str = "data/chess_archive.db"):
        """
        Initialize the database manager
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self._ensure_directory()
        self._initialize_database()
    
    def _ensure_directory(self):
        """Ensure the data directory exists"""
        directory = os.path.dirname(self.db_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
    
    def _get_connection(self):
        """Get a connection to the SQLite database"""
        return sqlite3.connect(self.db_path)
    
    def _initialize_database(self):
        """Create database tables if they don't exist"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Players table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS players (
            fide_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            rating INTEGER,
            title TEXT,
            federation TEXT,
            birth_year INTEGER,
            is_active INTEGER DEFAULT 1
        )
        ''')
        
        # Player accounts table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fide_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            username TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            last_update TEXT,
            total_games INTEGER DEFAULT 0,
            FOREIGN KEY (fide_id) REFERENCES players (fide_id),
            UNIQUE (fide_id, platform)
        )
        ''')
        
        # Collection logs table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS collection_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fide_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            time_period TEXT,
            games_count INTEGER,
            time_controls TEXT,
            status TEXT,
            error_message TEXT,
            timestamp TEXT,
            FOREIGN KEY (fide_id) REFERENCES players (fide_id)
        )
        ''')
        
        # Scheduled tasks table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS scheduled_tasks (
            job_id TEXT PRIMARY KEY,
            fide_id TEXT NOT NULL,
            platforms TEXT NOT NULL,
            day_of_month INTEGER,
            hour INTEGER,
            time_controls TEXT,
            max_games INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            FOREIGN KEY (fide_id) REFERENCES players (fide_id)
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def import_player_data(self, df: pd.DataFrame) -> bool:
        """
        Import player data from a DataFrame
        
        Args:
            df: DataFrame containing player data
            
        Returns:
            Success flag
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Import player data
            for _, row in df.iterrows():
                fide_id = row['fide_id']
                name = row['name']
                
                # Optional fields
                rating = row.get('rating') if 'rating' in row and not pd.isna(row.get('rating')) else None
                title = row.get('title') if 'title' in row and not pd.isna(row.get('title')) else None
                federation = row.get('federation') if 'federation' in row and not pd.isna(row.get('federation')) else None
                birth_year = row.get('birth_year') if 'birth_year' in row and not pd.isna(row.get('birth_year')) else None
                
                # Check if player exists
                cursor.execute('SELECT fide_id FROM players WHERE fide_id = ?', (fide_id,))
                player_exists = cursor.fetchone()
                
                if player_exists:
                    # Update existing player
                    cursor.execute('''
                    UPDATE players 
                    SET name = ?, rating = ?, title = ?, federation = ?, birth_year = ?
                    WHERE fide_id = ?
                    ''', (name, rating, title, federation, birth_year, fide_id))
                else:
                    # Insert new player
                    cursor.execute('''
                    INSERT INTO players (fide_id, name, rating, title, federation, birth_year)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', (fide_id, name, rating, title, federation, birth_year))
                
                # Handle player accounts
                if 'chesscom_username' in row and not pd.isna(row.get('chesscom_username')):
                    self._update_player_account(cursor, fide_id, 'chess.com', row['chesscom_username'])
                    
                if 'lichess_username' in row and not pd.isna(row.get('lichess_username')):
                    self._update_player_account(cursor, fide_id, 'lichess', row['lichess_username'])
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error importing player data: {str(e)}")
            return False
    
    def _update_player_account(self, cursor, fide_id: str, platform: str, username: str):
        """
        Insert or update a player account
        
        Args:
            cursor: SQLite cursor
            fide_id: FIDE ID of the player
            platform: Platform name (chess.com or lichess)
            username: Username on the platform
        """
        # Check if account exists
        cursor.execute(
            'SELECT id FROM player_accounts WHERE fide_id = ? AND platform = ?', 
            (fide_id, platform)
        )
        account_exists = cursor.fetchone()
        
        if account_exists:
            # Update existing account
            cursor.execute('''
            UPDATE player_accounts 
            SET username = ?
            WHERE fide_id = ? AND platform = ?
            ''', (username, fide_id, platform))
        else:
            # Insert new account
            cursor.execute('''
            INSERT INTO player_accounts (fide_id, platform, username, is_active, last_update, total_games)
            VALUES (?, ?, ?, 1, NULL, 0)
            ''', (fide_id, platform, username))
    
    def get_player_data(self) -> pd.DataFrame:
        """
        Get player data as a DataFrame
        
        Returns:
            DataFrame with player data and accounts
        """
        try:
            conn = self._get_connection()
            
            # Query players and their accounts
            query = '''
            SELECT 
                p.fide_id, 
                p.name, 
                p.rating, 
                p.title, 
                p.federation, 
                p.birth_year,
                cc.username as chesscom_username,
                li.username as lichess_username
            FROM 
                players p
            LEFT JOIN 
                player_accounts cc ON p.fide_id = cc.fide_id AND cc.platform = 'chess.com'
            LEFT JOIN 
                player_accounts li ON p.fide_id = li.fide_id AND li.platform = 'lichess'
            '''
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            return df
        except Exception as e:
            print(f"Error getting player data: {str(e)}")
            return pd.DataFrame()
    
    def log_collection(
        self, 
        fide_id: str, 
        platform: str, 
        time_period: str,
        games_count: int, 
        time_controls: List[str] = None,
        status: str = "success", 
        error_message: str = None
    ) -> bool:
        """
        Log a game collection attempt
        
        Args:
            fide_id: FIDE ID of the player
            platform: Platform name (chess.com or lichess)
            time_period: Time period for collection
            games_count: Number of games collected
            time_controls: List of time controls collected
            status: Status of collection (success or error)
            error_message: Optional error message
            
        Returns:
            Success flag
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Convert time_controls list to JSON string
            if time_controls:
                time_controls_json = json.dumps(time_controls)
            else:
                time_controls_json = None
            
            # Insert log record
            cursor.execute('''
            INSERT INTO collection_logs 
            (fide_id, platform, time_period, games_count, time_controls, status, error_message, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (fide_id, platform, time_period, games_count, time_controls_json, status, error_message, timestamp))
            
            # Update player account status
            cursor.execute('''
            UPDATE player_accounts
            SET is_active = ?, last_update = ?, total_games = total_games + ?
            WHERE fide_id = ? AND platform = ?
            ''', (1 if status == "success" else 0, timestamp, games_count, fide_id, platform))
            
            conn.commit()
            conn.close()
            
            return True
        except Exception as e:
            print(f"Error logging collection: {str(e)}")
            return False
    
    def save_scheduled_task(
        self,
        job_id: str,
        fide_id: str,
        platforms: List[str],
        day_of_month: int,
        hour: int,
        time_controls: List[str] = None,
        max_games: int = 0
    ) -> bool:
        """
        Save a scheduled task configuration
        
        Args:
            job_id: APScheduler job ID
            fide_id: FIDE ID of the player
            platforms: List of platforms to collect from
            day_of_month: Day of month to run
            hour: Hour of day to run
            time_controls: List of time controls to collect
            max_games: Maximum games to collect
            
        Returns:
            Success flag
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Convert lists to JSON strings
            platforms_json = json.dumps(platforms)
            time_controls_json = json.dumps(time_controls) if time_controls else None
            
            # Check if task exists
            cursor.execute('SELECT job_id FROM scheduled_tasks WHERE job_id = ?', (job_id,))
            task_exists = cursor.fetchone()
            
            if task_exists:
                # Update existing task
                cursor.execute('''
                UPDATE scheduled_tasks
                SET fide_id = ?, platforms = ?, day_of_month = ?, hour = ?, 
                    time_controls = ?, max_games = ?, is_active = 1
                WHERE job_id = ?
                ''', (fide_id, platforms_json, day_of_month, hour, time_controls_json, max_games, job_id))
            else:
                # Insert new task
                cursor.execute('''
                INSERT INTO scheduled_tasks
                (job_id, fide_id, platforms, day_of_month, hour, time_controls, max_games, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                ''', (job_id, fide_id, platforms_json, day_of_month, hour, time_controls_json, max_games))
            
            conn.commit()
            conn.close()
            
            return True
        except Exception as e:
            print(f"Error saving scheduled task: {str(e)}")
            return False
    
    def get_scheduled_tasks(self) -> pd.DataFrame:
        """
        Get all scheduled tasks
        
        Returns:
            DataFrame with scheduled tasks
        """
        try:
            conn = self._get_connection()
            
            # Query scheduled tasks with player names
            query = '''
            SELECT 
                t.job_id,
                t.fide_id,
                p.name as player_name,
                t.platforms,
                t.day_of_month,
                t.hour,
                t.time_controls,
                t.max_games,
                t.is_active
            FROM 
                scheduled_tasks t
            JOIN 
                players p ON t.fide_id = p.fide_id
            WHERE 
                t.is_active = 1
            '''
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            # Parse JSON columns
            if not df.empty:
                df['platforms'] = df['platforms'].apply(lambda x: json.loads(x) if x else [])
                df['time_controls'] = df['time_controls'].apply(lambda x: json.loads(x) if x else None)
            
            return df
        except Exception as e:
            print(f"Error getting scheduled tasks: {str(e)}")
            return pd.DataFrame()
    
    def delete_scheduled_task(self, job_id: str) -> bool:
        """
        Delete a scheduled task
        
        Args:
            job_id: APScheduler job ID
            
        Returns:
            Success flag
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Delete the task
            cursor.execute('DELETE FROM scheduled_tasks WHERE job_id = ?', (job_id,))
            
            conn.commit()
            conn.close()
            
            return True
        except Exception as e:
            print(f"Error deleting scheduled task: {str(e)}")
            return False
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """
        Get collection statistics
        
        Returns:
            Dictionary with statistics
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get total collections
            cursor.execute('SELECT COUNT(*) FROM collection_logs')
            total_collections = cursor.fetchone()[0]
            
            # Get total games collected
            cursor.execute('SELECT SUM(games_count) FROM collection_logs')
            total_games = cursor.fetchone()[0] or 0
            
            # Get collections by platform
            cursor.execute('''
            SELECT platform, COUNT(*), SUM(games_count)
            FROM collection_logs
            GROUP BY platform
            ''')
            platform_stats = {
                platform: {"collections": count, "games": games or 0}
                for platform, count, games in cursor.fetchall()
            }
            
            # Get success/error counts
            cursor.execute('''
            SELECT status, COUNT(*)
            FROM collection_logs
            GROUP BY status
            ''')
            status_stats = {status: count for status, count in cursor.fetchall()}
            
            # Get recent collections
            cursor.execute('''
            SELECT 
                l.fide_id, 
                p.name as player_name,
                l.platform,
                l.time_period, 
                l.games_count, 
                l.status, 
                l.timestamp
            FROM 
                collection_logs l
            JOIN 
                players p ON l.fide_id = p.fide_id
            ORDER BY 
                l.timestamp DESC
            LIMIT 20
            ''')
            
            recent_collections = []
            for row in cursor.fetchall():
                recent_collections.append({
                    "fide_id": row[0],
                    "player_name": row[1],
                    "platform": row[2],
                    "time_period": row[3],
                    "games_count": row[4],
                    "status": row[5],
                    "timestamp": row[6]
                })
            
            conn.close()
            
            return {
                "total_collections": total_collections,
                "total_games": total_games,
                "platform_stats": platform_stats,
                "status_stats": status_stats,
                "recent_collections": recent_collections
            }
        except Exception as e:
            print(f"Error getting collection stats: {str(e)}")
            return {}
    
    def get_inactive_accounts(self) -> pd.DataFrame:
        """
        Get all inactive accounts
        
        Returns:
            DataFrame with inactive accounts and their last update
        """
        try:
            conn = self._get_connection()
            
            # Query inactive accounts with player names
            query = '''
            SELECT 
                a.fide_id,
                p.name as player_name,
                a.platform,
                a.username,
                a.last_update,
                a.total_games
            FROM 
                player_accounts a
            JOIN 
                players p ON a.fide_id = p.fide_id
            WHERE 
                a.is_active = 0
            '''
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            return df
        except Exception as e:
            print(f"Error getting inactive accounts: {str(e)}")
            return pd.DataFrame()
