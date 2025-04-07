import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, List

def display_collection_stats(stats: Dict[str, Any]):
    """
    Display archive statistics
    
    Args:
        stats: Dictionary with archive statistics
    """
    st.subheader("Archive Overview")
    
    # Display high-level metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Players", stats.get("total_players", 0))
    
    with col2:
        st.metric("Total Games", stats.get("total_games", 0))
    
    with col3:
        platforms = stats.get("games_by_platform", {})
        chess_com_games = platforms.get("chess.com", 0)
        lichess_games = platforms.get("lichess", 0)
        
        if chess_com_games + lichess_games > 0:
            chess_com_pct = round(chess_com_games / (chess_com_games + lichess_games) * 100)
            lichess_pct = 100 - chess_com_pct
            st.metric("Platform Split", f"Chess.com: {chess_com_pct}%, Lichess: {lichess_pct}%")
        else:
            st.metric("Platform Split", "No games collected")
    
    # Games by platform chart
    st.subheader("Games by Platform")
    
    platforms = stats.get("games_by_platform", {})
    platform_df = pd.DataFrame({
        "Platform": list(platforms.keys()),
        "Games": list(platforms.values())
    })
    
    if not platform_df.empty and platform_df["Games"].sum() > 0:
        fig_platform = px.bar(
            platform_df, 
            x="Platform", 
            y="Games",
            color="Platform",
            color_discrete_map={"chess.com": "#7FA650", "lichess": "#4D4D4D"}
        )
        st.plotly_chart(fig_platform, use_container_width=True)
    else:
        st.info("No games collected yet.")
    
    # Games by year chart
    st.subheader("Games by Year")
    
    years = stats.get("games_by_year", {})
    year_df = pd.DataFrame({
        "Year": list(years.keys()),
        "Games": list(years.values())
    })
    
    if not year_df.empty and year_df["Games"].sum() > 0:
        # Sort by year
        year_df["Year"] = year_df["Year"].astype(int)
        year_df = year_df.sort_values("Year")
        
        fig_year = px.line(
            year_df, 
            x="Year", 
            y="Games",
            markers=True
        )
        st.plotly_chart(fig_year, use_container_width=True)
    else:
        st.info("No games collected yet.")
    
    # Player statistics
    st.subheader("Player Statistics")
    
    players = stats.get("players", [])
    
    if players:
        player_stats = []
        
        for player in players:
            player_name = player.get("name", "Unknown")
            fide_id = player.get("fide_id", "")
            
            platforms = player.get("platforms", {})
            chess_com_games = platforms.get("chess.com", {}).get("total_games", 0) if "chess.com" in platforms else 0
            lichess_games = platforms.get("lichess", {}).get("total_games", 0) if "lichess" in platforms else 0
            total_games = chess_com_games + lichess_games
            
            last_update = None
            for platform, platform_info in platforms.items():
                platform_update = platform_info.get("last_update")
                if platform_update and (last_update is None or platform_update > last_update):
                    last_update = platform_update
            
            player_stats.append({
                "Player": player_name,
                "FIDE ID": fide_id,
                "Total Games": total_games,
                "Chess.com Games": chess_com_games,
                "Lichess Games": lichess_games,
                "Last Update": last_update or "Never"
            })
        
        # Create DataFrame and sort by total games
        player_df = pd.DataFrame(player_stats)
        player_df = player_df.sort_values("Total Games", ascending=False)
        
        st.dataframe(player_df)
        
        # Show top players chart
        st.subheader("Top Players by Games Collected")
        
        top_players = player_df.head(10)
        
        if not top_players.empty and top_players["Total Games"].sum() > 0:
            fig_top = px.bar(
                top_players,
                x="Player",
                y=["Chess.com Games", "Lichess Games"],
                title="Top 10 Players by Games Collected",
                labels={"value": "Games", "variable": "Platform"}
            )
            st.plotly_chart(fig_top, use_container_width=True)
    else:
        st.info("No player data available.")
