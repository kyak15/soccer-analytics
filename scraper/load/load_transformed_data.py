"""
Purpose: Handles safely inserting match, team, player, and player_match_stats tranforsmed data into the database

--- File Notes --- 

1. In functions that check for existing data, SELECT EXISTS (SELECT 1 FROM..) is used for the following reasons:
    - EXISTS stops as soon as it finds one matching row; doesn't scan the whole table or ret data
    - SELECT 1 avoids returning column data. 
    - EXISTS return true/false 
"""

import os
import json
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extras import execute_batch


def get_db_connection():
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([database, user, password]):
        raise ValueError(
            "Database connection requires DB_NAME, DB_USER, and DB_PASSWORD environment variables"
        )

    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )


def match_exists(conn, match_id: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS(SELECT 1 FROM match WHERE match_id = %s)", (match_id,))
        return cur.fetchone()[0]


def team_exists(conn, team_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS(SELECT 1 FROM team WHERE id = %s)", (team_id,))
        return cur.fetchone()[0]


def player_exists(conn, player_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS(SELECT 1 FROM player WHERE id = %s)", (player_id,))
        return cur.fetchone()[0]


def get_existing_teams(conn, team_ids: List[int]) -> set:
    if not team_ids:
        return set()
    
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM team WHERE id = ANY(%s)",
            (team_ids,)
        )
        return {row[0] for row in cur.fetchall()}


def get_existing_players(conn, player_ids: List[int]) -> set:
    if not player_ids:
        return set()
    
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM player WHERE id = ANY(%s)",
            (player_ids,)
        )
        return {row[0] for row in cur.fetchall()}


def load_teams(conn, teams_data: Dict[str, Any]) -> None:
    """
    Insert teams into the database if they don't already exist.
    Checks existence before inserting to avoid unnecessary database operations.
    Note: Does NOT commit - transaction management is handled by the caller.
    """
    teams_to_insert = [
        teams_data["homeTeam"],
        teams_data["awayTeam"]
    ]

    # Check which teams already exist
    team_ids = [team["team_id"] for team in teams_to_insert]
    existing_teams = get_existing_teams(conn, team_ids) # returns list of team ids that exist in database
    new_teams = [team for team in teams_to_insert if team["team_id"] not in existing_teams]
    
    # no new teams exist, so writing does not need to occur below 
    if not new_teams:
        print(f"  ⊘ All {len(teams_to_insert)} teams already exist")
        return

    # new teams exist, so writing must occur
    with conn.cursor() as cur:
        for team in new_teams:
            cur.execute(
                """
                INSERT INTO team (id, name, logo_url)
                VALUES (%s, %s, %s)
                """,
                (
                    team["team_id"],
                    team["team_name"],
                    team.get("team_logo")
                )
            )
    
    print(f"Prepared {len(new_teams)} new teams for insertion ({len(existing_teams)} already existed)")


def load_players(conn, players_data: Dict[str, Any]) -> None:
    """
    Insert new players into the database if they don't already exist.
    Checks existence first - only inserts new players, skips existing ones.
    Note: Does NOT commit - transaction management is handled by the caller.
    """
    all_players = players_data["home_team_players"] + players_data["away_team_players"]
    
    # Remove duplicates by player_id (in case a player appears in both lists)
    unique_players = {}
    for player in all_players:
        pid = player["player_id"]
        if pid not in unique_players:
            unique_players[pid] = player

    players_list = list(unique_players.values())
    
    # Check which players already exist
    player_ids = [player["player_id"] for player in players_list]
    existing_players = get_existing_players(conn, player_ids)
    
    new_players = [p for p in players_list if p["player_id"] not in existing_players]
    
    if not new_players:
        print(f"  ⊘ All {len(players_list)} players already exist")
        return

    with conn.cursor() as cur:
        for player in new_players:
            first_name = player.get("first_name") or ""
            last_name = player.get("last_name") or ""
            full_name = f"{first_name} {last_name}".strip() if (first_name or last_name) else None
            
            cur.execute(
                """
                INSERT INTO player (id, first_name, last_name, full_name, nationality)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    player["player_id"],
                    first_name,
                    last_name,
                    full_name,
                    player.get("nationality")
                )
            )
    
    print(f"Prepared {len(new_players)} new players for insertion ({len(existing_players)} already existed)")


def load_match(conn, match_data: Dict[str, Any]) -> bool:
    """
    Insert match into the database if it doesn't already exist.
    Returns True if match was inserted, False if it already existed.
    Note: Does NOT commit - transaction management is handled by the caller.
    """
    match = match_data["match"]
    
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO match (match_id, matchday, match_date, home_team_id, away_team_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (match_id) DO NOTHING
            RETURNING match_id
            """,
            (
                match["match_id"],
                match["match_round"],
                match["match_date"],
                match["home_team_id"],
                match["away_team_id"]
            )
        )
        result = cur.fetchone()
        
        if result:
            print(f"  ✓ Prepared match {match['match_id']} for insertion")
            return True
        else:
            print(f"  ⊘ Match {match['match_id']} already exists, skipping")
            return False


def load_player_match_stats(conn, player_stats_data: List[Dict[str, Any]]) -> None:
    """
    Insert player match stats into the database.
    Assumes match doesn't exist (should only be called if match was just inserted).
    Uses ON CONFLICT DO NOTHING to handle potential duplicate (match_id, player_id) pairs.
    Note: Does NOT commit - transaction management is handled by the caller.
    """
    if not player_stats_data:
        print("  ⊘ No player stats to load")
        return

    # get all column names from the first player stat entry and remove un-needed columns
    first_stat = player_stats_data[0]
    columns = list(first_stat.keys())
    columns_to_remove = ["first_name", "last_name"]
    columns = [col for col in columns if col not in columns_to_remove]
    
    # build INSERT statement 
    placeholders = ", ".join(["%s"] * len(columns))
    column_names = ", ".join(columns)
    
    insert_query = f"""
        INSERT INTO player_match_stats ({column_names})
        VALUES ({placeholders})
        ON CONFLICT (match_id, player_id) DO NOTHING
    """
    
    # prep data arows 
    rows = []
    for stat in player_stats_data:
        row = [stat.get(col) for col in columns]
        rows.append(row)
    
    # execute batch used to insert large amount of rows to prevent overhead 
    with conn.cursor() as cur:
        execute_batch(cur, insert_query, rows)
    
    print(f"Prepared {len(player_stats_data)} player match stats for insertion")


def load_transformed_match(transformed_data: Dict[str, Any], conn: Optional[Any] = None) -> bool:
    """
    Load a single transformed match into the database.
    
    This function implements proper ACID transaction management:
    - All operations (teams, players, match, player_stats) are in a single transaction
    - Either ALL data is committed, or NONE is committed (atomicity)
    - If any step fails, the entire transaction is rolled back
    
    Returns:
        True if match was loaded, False if match already existed (skipped)
    """
    should_close_conn = False
    if conn is None:
        conn = get_db_connection()
        should_close_conn = True
    
    match_id = transformed_data.get("match", {}).get("match_id", "unknown")
    
    try:
        # Check if match already exists (read-only check, no transaction needed)
        if match_exists(conn, match_id):
            print(f"Match {match_id} already exists in database, skipping...")
            return False
        
        print(f"\nLoading match {match_id} into database...")
        
        # Start transaction 
        load_teams(conn, transformed_data["teams"])
        load_players(conn, transformed_data["players"])
        match_inserted = load_match(conn, transformed_data["match"])
        
        # Match was not inserted: Rollback everything since we can't complete the load
        if not match_inserted:
            conn.rollback()
            print(f"⊘ Match {match_id} was not inserted (may have been inserted concurrently) - rolled back all changes")
            return False
        
        # Only load player stats if match was actually inserted
        load_player_match_stats(conn, transformed_data["player_stats"])
        
        conn.commit()
        print(f"Successfully loaded match {match_id} (all data committed atomically)")
        
        return True
        
    except Exception as e:
        # Rollback the entire transaction on any error
        conn.rollback()
        print(f"Error loading match {match_id}: {e}")
        print(f"All changes rolled back (atomicity maintained)")
        raise
    finally:
        if should_close_conn:
            conn.close()


def load_transformed_match_from_file(file_path: str, conn: Optional[Any] = None) -> bool:
    with open(file_path, "r") as f:
        transformed_data = json.load(f)
    
    return load_transformed_match(transformed_data, conn)

