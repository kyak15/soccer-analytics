from typing import Dict, Any, List, Tuple, Optional
import os

POSITION_ID_MAP: Dict[int, Tuple[str, str]] = {
    # GK
    11: ("GK", "GK"),

    # DEF
    32: ("RB", "DF"),
    33: ("CB", "DF"),
    34: ("CB", "DF"),
    35: ("CB", "DF"),
    36: ("CB", "DF"),
    37: ("CB", "DF"),
    38: ("LB", "DF"),
    62: ("RWB", "DF"),
    51: ("RWB", "DF"),
    59: ("LWB", "DF"),
    68: ("LWB", "DF"),

    # MID
    66: ("DM", "MF"),
    64: ("DM", "MF"),
    65: ("DM", "MF"),

    83:  ("RW", "MF"),
    103: ("RW", "MF"),

    84: ("AM", "MF"),
    85: ("AM", "MF"),
    86: ("AM", "MF"),

    78: ("LM", "MF"),
    79: ("LM", "MF"),

    72: ("RM", "MF"),
    71: ("RM", "MF"),

    73: ("CM", "MF"),
    74: ("CM", "MF"),
    75: ("CM", "MF"),
    76: ("CM", "MF"),
    77: ("CM", "MF"),

    # FW
    87:  ("LW", "FW"),
    107: ("LW", "FW"),
    
    115: ("ST", "FW"),
    106: ("ST", "FW"),
    104: ("ST", "FW"),
    105: ("ST", "FW"),
}

# FotMob's usualPlayingPositionId mapping: 0=GK, 1=DF, 2=MF, 3=FW
USUAL_POSITION_ID_MAP: Dict[int, Tuple[str, str]] = {
    0: ("GK", "GK"),
    1: ("DF", "DF"),
    2: ("MF", "MF"),
    3: ("FW", "FW"),
}

def transform_full_match(raw_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Primary function that handles data transformation. takes raw json data and transforms it into cleaner data to be inserted into database. 
    """

    match_details = raw_json["matchDetails"]
    player_stats_raw = raw_json.get("playerStats", {})
    
    # Fallback method incase playerStats is empty
    if not player_stats_raw and "content" in match_details:
        content = match_details.get("content", {})
        if "playerStats" in content:
            player_stats_raw = content["playerStats"]
            print(f"Using playerStats from matchDetails.content.playerStats ({len(player_stats_raw)} players)")

    match_data = clean_match_data(match_details)          
    team_data = clean_team_data(match_details)            
    player_info_data = clean_player_info_data(match_details)  

    match_id = match_data["match"]["match_id"]
    player_stats_data = clean_player_stats_data(match_details=match_details, player_stats_raw=player_stats_raw, match_id=match_id)

    return {
        "match": match_data["match"],
        "teams": team_data,
        "players": player_info_data,
        "player_stats": player_stats_data,
    }

def clean_team_data(match_details: Dict[str, Any]) -> Dict[str, Any]:
    team_data: Dict[str, Any] = {}

    home = match_details["header"]["teams"][0]
    away = match_details["header"]["teams"][1]

    team_data["homeTeam"] = {
        "team_id": home["id"],
        "team_name": home["name"],
        "team_logo": home.get("imageUrl"),
    }

    team_data["awayTeam"] = {
        "team_id": away["id"],
        "team_name": away["name"],
        "team_logo": away.get("imageUrl"),
    }

    return team_data


def clean_match_data(match_details: Dict[str, Any]) -> Dict[str, Any]:
    """
    Obtain specific match data such as round, teams, and date
    """
    mf = match_details["content"]["matchFacts"]

    match_id_raw = mf["matchId"]
    
    try:
        match_id = int(match_id_raw)
    except (TypeError, ValueError):
        match_id = match_id_raw

    info_box = mf["infoBox"]

    match_round = info_box["Tournament"]["round"]
    match_date = info_box["Match Date"]["utcTime"]

    home_team_id = match_details["header"]["teams"][0]["id"]
    away_team_id = match_details["header"]["teams"][1]["id"]

    return {
        "match": {
            "match_id": match_id,
            "match_round": match_round,
            "match_date": match_date,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
        }
    }


def clean_player_info_data(match_details: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract player identity + basic meta (per team). 
    Only includes subs who actually played (performance != None).
    """

    player_info_data: Dict[str, Any] = {}
    home_players: List[Dict[str, Any]] = []
    away_players: List[Dict[str, Any]] = []

    home_team = match_details["content"]["lineup"]["homeTeam"]
    away_team = match_details["content"]["lineup"]["awayTeam"]

    home_team_id = home_team["id"]
    away_team_id = away_team["id"]

    raw_home_team_starters = home_team["starters"]
    raw_home_team_subs = home_team["subs"]

    raw_away_team_starters = away_team["starters"]
    raw_away_team_subs = away_team["subs"]

    for starter in raw_home_team_starters:
        home_players.append(_extract_player_identity(starter, home_team_id))

    for sub in raw_home_team_subs:
        if sub.get("performance") is not None:
            home_players.append(_extract_player_identity(sub, home_team_id))

    for starter in raw_away_team_starters:
        away_players.append(_extract_player_identity(starter, away_team_id))

    for sub in raw_away_team_subs:
        if sub.get("performance") is not None:
            away_players.append(_extract_player_identity(sub, away_team_id))

    player_info_data["home_team_players"] = home_players
    player_info_data["away_team_players"] = away_players

    return player_info_data


def _get_position_from_ids(position_id: Optional[int], usual_pos_id: Optional[int]) -> Tuple[str, str]:
    """
    Get position mapping from positionId (for starters) or usualPlayingPositionId (for subs).
    Returns (raw_position, normalized_position) tuple.
    """
    # First try positionId (for starters)
    if position_id is not None:
        result = POSITION_ID_MAP.get(position_id)
        if result:
            return result
    
    # Fallback to usualPlayingPositionId (for substitutes)
    if usual_pos_id is not None:
        result = USUAL_POSITION_ID_MAP.get(usual_pos_id)
        if result:
            return result
    
    # Default to UNK if neither is available
    return ("UNK", "UNK")


def _extract_player_identity(player_node: Dict[str, Any], team_id: int) -> Dict[str, Any]:
    """
    Helper Function: given a lineup player block (starter/sub) + team_id,
    extract consistent identity fields.
    """
    pid = player_node["id"]
    first_name = player_node.get("firstName")
    last_name = player_node.get("lastName")
    nationality = player_node.get("countryName")
    position_id = player_node.get("positionId")
    usual_pos_id = player_node.get("usualPlayingPositionId")
    shirt = player_node.get("shirtNumber")

    raw_pos, norm_pos = _get_position_from_ids(position_id, usual_pos_id)

    return {
        "player_id": pid,
        "team_id": team_id,
        "first_name": first_name,
        "last_name": last_name,
        "nationality": nationality,
        "position_id": position_id,
        "usual_position_id": usual_pos_id,
        "shirt_number": shirt,
        "raw_position": raw_pos,
        "normalized_position": norm_pos,
    }


def extract_stat(
    stats_dict: Dict[str, Any],
    label: str,
    fraction: bool = False,
) -> Optional[Any]:
    """
    Retrieve a stat from the flattened stats dict.

    - If fraction=False:
        returns `value` or None.
    - If fraction=True:
        returns (value, total) or (None, None).

    where `stats_dict[label]` is of form:
      { "key": "...", "stat": { "value": X, "total": Y?, "type": ... } }
    """
    node = stats_dict.get(label)
    if not node:
        return (None, None) if fraction else None

    stat = node.get("stat", {})

    if fraction:
        return stat.get("value"), stat.get("total")

    return stat.get("value")


def extract_stat_with_variants(
    stats_dict: Dict[str, Any],
    label_variants: List[str],
) -> Optional[Any]:
    """
    Try multiple label variations to extract a stat.
    Returns the first match found, or None if none match.
    """
    for label in label_variants:
        result = extract_stat(stats_dict, label)
        if result is not None:
            return result
    return None


def collect_player_stats(raw_player_stats_entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten FotMob playerStats structure into: {label -> statNode}.
    """
    flat: Dict[str, Any] = {}

    # Debug: check if entry is empty
    if not raw_player_stats_entry:
        return flat

    # Check if the structure has "stats" key
    stats_list = raw_player_stats_entry.get("stats", [])
    if not stats_list:
        if isinstance(raw_player_stats_entry, dict) and "stats" not in raw_player_stats_entry:
            return raw_player_stats_entry

    for category in stats_list:
        group_stats = category.get("stats", {})
        for label, data in group_stats.items():
            flat[label] = data

    return flat

def calculate_player_goalkeeper_score(player_stats):
    gk_score_list = []

    gk_score_list.append(player_stats["saves"] * 0.15 if player_stats["saves"] is not None else 0)
    gk_score_list.append(player_stats["saves_in_box"] * 0.10 if player_stats["saves_in_box"] is not None else 0)
    gk_score_list.append(player_stats["diving_save"] * 0.05 if player_stats["diving_save"] is not None else 0)
    gk_score_list.append(player_stats["high_claim"] * 0.05 if player_stats["high_claim"] is not None else 0)
    gk_score_list.append(player_stats["punches"] * 0.03 if player_stats["punches"] is not None else 0)
    gk_score_list.append(player_stats["act_as_sweeper"] * 0.05 if player_stats["act_as_sweeper"] is not None else 0)
    gk_score_list.append(player_stats["goals_conceded"] * -0.10 if player_stats["goals_conceded"] is not None else 0)
    
    # Critical mistakes - apply significant negative weights
    gk_score_list.append(player_stats["own_goals"] * -0.50 if player_stats["own_goals"] is not None else 0)
    gk_score_list.append(player_stats["errors_leading_to_goal"] * -0.30 if player_stats["errors_leading_to_goal"] is not None else 0)

    # Passing stats
    gk_score_list.append(player_stats["passes_completed"] * 0.06 if player_stats["passes_completed"] is not None else 0)
    if player_stats["passes_completed"] is not None and player_stats["passes_attempted"] is not None and player_stats["passes_attempted"] > 0:
        pass_completion_pct = (player_stats["passes_completed"] / player_stats["passes_attempted"]) * 0.06
        gk_score_list.append(pass_completion_pct)
    gk_score_list.append(player_stats["passes_into_final_third"] * 0.05 if player_stats["passes_into_final_third"] is not None else 0)

    # Long passing stats
    gk_score_list.append(player_stats["long_balls_completed"] * 0.08 if player_stats["long_balls_completed"] is not None else 0)
    if player_stats["long_balls_completed"] is not None and player_stats["long_balls_attempted"] is not None and player_stats["long_balls_attempted"] > 0:
        long_ball_completion_pct = (player_stats["long_balls_completed"] / player_stats["long_balls_attempted"]) * 0.08
        gk_score_list.append(long_ball_completion_pct)

    # Normalize goalkeeper score to match outfield player scale (multiply by 2.5)
    # This brings GK scores from 0-4 range to 0-10 range, similar to outfield final scores
    raw_gk_score = sum(gk_score_list)
    return raw_gk_score * 2.5

def calculate_player_defense_score(player_stats, team_goals_conceded: int = 0):

    defense_score_list = []

    # Defensive actions - comprehensive metric that captures overall defensive contribution
    # Weighted higher since it represents total defensive involvement
    defense_score_list.append(player_stats["defensive_actions"] * 0.10 if player_stats["defensive_actions"] is not None else 0)

    defense_score_list.append(player_stats["tackles"] * 0.12 if player_stats["tackles"] is not None else 0)
    defense_score_list.append(player_stats["last_man_tackles"] * 0.10 if player_stats["last_man_tackles"] is not None else 0)
    defense_score_list.append(player_stats["blocks"] * 0.08 if player_stats["blocks"] is not None else 0)
    # Reduced clearance weights - high clearances can indicate being under pressure
    defense_score_list.append(player_stats["clearances"] * 0.03 if player_stats["clearances"] is not None else 0)
    defense_score_list.append(player_stats["headed_clearances"] * 0.03 if player_stats["headed_clearances"] is not None else 0)
    defense_score_list.append(player_stats["interceptions"] * 0.10 if player_stats["interceptions"] is not None else 0)
    defense_score_list.append(player_stats["recoveries"] * 0.07 if player_stats["recoveries"] is not None else 0)
    defense_score_list.append(player_stats["dribbled_past"] * -0.06 if player_stats["dribbled_past"] is not None else 0)
    defense_score_list.append(player_stats["fouls_committed"] * -0.06 if player_stats["fouls_committed"] is not None else 0)

    if player_stats["ground_duels_completed"] is not None and player_stats["ground_duels_attempted"] is not None and player_stats["ground_duels_attempted"] > 0:
        ground_duels_percent = (player_stats["ground_duels_completed"] / player_stats["ground_duels_attempted"]) * 0.12
        defense_score_list.append(ground_duels_percent)
    if player_stats["aerial_duels_completed"] is not None and player_stats["aerial_duels_attempted"] is not None and player_stats["aerial_duels_attempted"] > 0:
        aerial_duels_percent = (player_stats["aerial_duels_completed"] / player_stats["aerial_duels_attempted"]) * 0.10
        defense_score_list.append(aerial_duels_percent)
    
    # Critical mistakes - apply significant negative weights
    defense_score_list.append(player_stats["own_goals"] * -0.50 if player_stats["own_goals"] is not None else 0)
    defense_score_list.append(player_stats["errors_leading_to_goal"] * -0.30 if player_stats["errors_leading_to_goal"] is not None else 0)
    
    # Team context: Penalty for goals conceded, bonus for clean sheet
    if team_goals_conceded == 0:
        # Clean sheet bonus - significant reward for keeping a clean sheet
        defense_score_list.append(0.75)
    else:
        # Penalty for goals conceded - shared responsibility among defenders
        # -0.20 per goal conceded
        defense_score_list.append(team_goals_conceded * -0.20)
    
    return sum(defense_score_list)

def calculate_player_midfield_score(player_stats):
    mid_score_list = []

    # Goals and assists are critical for midfielders - add significant weight
    mid_score_list.append(player_stats["goals"] * 0.40 if player_stats["goals"] is not None else 0)
    mid_score_list.append(player_stats["assists"] * 0.25 if player_stats["assists"] is not None else 0)

    mid_score_list.append(player_stats["passes_completed"] * 0.08 if player_stats["passes_completed"] is not None else 0)
    if player_stats["passes_completed"] is not None and player_stats["passes_attempted"] is not None and player_stats["passes_attempted"] > 0:
        pass_accuracy = (player_stats["passes_completed"] / player_stats["passes_attempted"]) * 0.12
        mid_score_list.append(pass_accuracy)
    mid_score_list.append(player_stats["long_balls_completed"] * 0.08 if player_stats["long_balls_completed"] is not None else 0)
    mid_score_list.append(player_stats["crosses_completed"] * 0.05 if player_stats["crosses_completed"] is not None else 0)
    mid_score_list.append(player_stats["passes_into_final_third"] * 0.15 if player_stats["passes_into_final_third"] is not None else 0)
    mid_score_list.append(player_stats["touches"] * 0.01 if player_stats["touches"] is not None else 0)
    mid_score_list.append(player_stats["was_fouled"] * 0.05 if player_stats["was_fouled"] is not None else 0)
    mid_score_list.append(player_stats["dispossesed"] * -0.05 if player_stats["dispossesed"] is not None else 0)
    mid_score_list.append(player_stats["chances_created"] * 0.20 if player_stats["chances_created"] is not None else 0)
    mid_score_list.append(player_stats["dribbles_completed"] * 0.07 if player_stats["dribbles_completed"] is not None else 0)
    mid_score_list.append(player_stats["touches_in_opp_box"] * 0.03 if player_stats["touches_in_opp_box"] is not None else 0)

    # Critical mistakes - apply significant negative weights
    mid_score_list.append(player_stats["own_goals"] * -0.50 if player_stats["own_goals"] is not None else 0)
    mid_score_list.append(player_stats["errors_leading_to_goal"] * -0.30 if player_stats["errors_leading_to_goal"] is not None else 0)

    return sum(mid_score_list)

def calculate_player_forward_score(player_stats):
    att_score_list = []

    # Significantly increase weights for goals and assists - these are the most important attacking contributions
    att_score_list.append(player_stats["goals"] * 0.60 if player_stats["goals"] is not None else 0)
    att_score_list.append(player_stats["assists"] * 0.30 if player_stats["assists"] is not None else 0)
    att_score_list.append(player_stats["shots_on_target"] * 0.15 if player_stats["shots_on_target"] is not None else 0)
    att_score_list.append(player_stats["total_shots"] * 0.05 if player_stats["total_shots"] is not None else 0)
    att_score_list.append(player_stats["chances_created"] * 0.10 if player_stats["chances_created"] is not None else 0)
    att_score_list.append(player_stats["dribbles_completed"] * 0.10 if player_stats["dribbles_completed"] is not None else 0)

    if player_stats["dribbles_completed"] is not None and player_stats["dribbles_attempted"] is not None and player_stats["dribbles_attempted"] > 0:
        dribble_success_rate = (player_stats["dribbles_completed"] / player_stats["dribbles_attempted"]) * 0.05
        att_score_list.append(dribble_success_rate)

    att_score_list.append(player_stats["touches_in_opp_box"] * 0.05 if player_stats["touches_in_opp_box"] is not None else 0)
    att_score_list.append(player_stats["penalties_won"] * 0.08 if player_stats["penalties_won"] is not None else 0)
    att_score_list.append(player_stats["crosses_completed"] * 0.04 if player_stats["crosses_completed"] is not None else 0)
    att_score_list.append(player_stats["long_balls_completed"] * 0.02 if player_stats["long_balls_completed"] is not None else 0)

    if player_stats["ground_duels_completed"] is not None and player_stats["ground_duels_attempted"] is not None and player_stats["ground_duels_attempted"] > 0:
        ground_duel_win_percent = (player_stats["ground_duels_completed"] / player_stats["ground_duels_attempted"]) * 0.05
        att_score_list.append(ground_duel_win_percent)
        
    if player_stats["aerial_duels_completed"] is not None and player_stats["aerial_duels_attempted"] is not None and player_stats["aerial_duels_attempted"] > 0:
        aerial_duel_win_percent = (player_stats["aerial_duels_completed"] / player_stats["aerial_duels_attempted"]) * 0.05
        att_score_list.append(aerial_duel_win_percent)

    # Critical mistakes - apply significant negative weights
    att_score_list.append(player_stats["own_goals"] * -0.50 if player_stats["own_goals"] is not None else 0)
    att_score_list.append(player_stats["errors_leading_to_goal"] * -0.30 if player_stats["errors_leading_to_goal"] is not None else 0)

    return sum(att_score_list) 

def calculate_final_score(pos, def_score, mid_score, off_score):
    """
    DF Weights: DEF 0.65, MID 0.25, FWD 0.10
    MF Weights: DEF 0.20, MID 0.50, FWD 0.30 (increased FWD weight to better reward goals/assists)
    FW Weights: DEF 0.10, MID 0.20, FWD 0.70
    """
    final_rating = 0 
    if pos == "DF":
        final_rating = (def_score * 0.65) + (mid_score * 0.25) + (off_score * 0.10)
    elif pos == "MF":
        final_rating = (def_score * 0.20) + (mid_score * 0.50) + (off_score * 0.30)
    elif pos == "FW":
        final_rating = (def_score * 0.1) + (mid_score * 0.20) + (off_score * 0.70)
    return final_rating 

def print_sorted_players_by_scores(cleaned_rows: List[Dict[str, Any]], match_id: Any):
    """
    Save players sorted by defense, midfield, forward, and final scores for a match to a file.
    Shows position-specific rankings, then mixed rankings, then final scores.
    """
    # Create logs directory if it doesn't exist
    logs_dir = "scraper/logs"
    os.makedirs(logs_dir, exist_ok=True)
    
    # Open file in append mode so all matches are saved
    rankings_file = os.path.join(logs_dir, "player_rankings.txt")
    
    with open(rankings_file, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"MATCH {match_id} - Player Rankings\n")
        f.write(f"{'='*80}\n")
        
        # Filter out players with no stats (all None)
        valid_players = [p for p in cleaned_rows if p.get("final_score") is not None]
        
        if not valid_players:
            f.write("No player stats available for this match.\n")
            return
        
        # Helper function to get player name
        def get_player_name(player):
            first = player.get("first_name") or ""
            last = player.get("last_name") or ""
            return f"{first} {last}".strip() or f"Player {player.get('player_id')}"
        
        # Helper function to write a ranking table
        def write_ranking_table(title: str, players: List[Dict[str, Any]], score_key: str, top_n: int = 10):
            if not players:
                return
            sorted_players = sorted(players, key=lambda x: x.get(score_key, 0), reverse=True)
            f.write(f"\n{title} ({len(sorted_players)} players):\n")
            f.write(f"{'Rank':<6} {'Player':<30} {'Position':<8} {score_key.replace('_', ' ').title():<20} {'Final Score':<15}\n")
            f.write("-" * 80 + "\n")
            for i, player in enumerate(sorted_players[:top_n], 1):
                name = get_player_name(player)
                pos = player.get("normalized_position", "N/A")
                score = player.get(score_key, 0)
                final_score = player.get("final_score", 0)
                f.write(f"{i:<6} {name:<30} {pos:<8} {score:<20.2f} {final_score:<15.2f}\n")
        
        # ========== POSITION-SPECIFIC RANKINGS ==========
        f.write(f"\n{'='*80}\n")
        f.write("POSITION-SPECIFIC RANKINGS\n")
        f.write(f"{'='*80}\n")
        
        # Get players by position (excluding GK)
        df_players = [p for p in valid_players if p.get("normalized_position") == "DF" and p.get("defense_score") is not None]
        mf_players = [p for p in valid_players if p.get("normalized_position") == "MF" and p.get("defense_score") is not None]
        fw_players = [p for p in valid_players if p.get("normalized_position") == "FW" and p.get("defense_score") is not None]
        
        # Defense scores by position
        write_ranking_table("🛡️  TOP DEFENSE SCORES - DEFENDERS", df_players, "defense_score")
        write_ranking_table("🛡️  TOP DEFENSE SCORES - MIDFIELDERS", mf_players, "defense_score")
        write_ranking_table("🛡️  TOP DEFENSE SCORES - FORWARDS", fw_players, "defense_score")
        
        # Midfield scores by position
        write_ranking_table("🎯 TOP MIDFIELD SCORES - DEFENDERS", df_players, "midfield_score")
        write_ranking_table("🎯 TOP MIDFIELD SCORES - MIDFIELDERS", mf_players, "midfield_score")
        write_ranking_table("🎯 TOP MIDFIELD SCORES - FORWARDS", fw_players, "midfield_score")
        
        # Forward scores by position
        write_ranking_table("⚽ TOP FORWARD SCORES - DEFENDERS", df_players, "forward_score")
        write_ranking_table("⚽ TOP FORWARD SCORES - MIDFIELDERS", mf_players, "forward_score")
        write_ranking_table("⚽ TOP FORWARD SCORES - FORWARDS", fw_players, "forward_score")
        
        # ========== MIXED RANKINGS (ALL POSITIONS) ==========
        f.write(f"\n{'='*80}\n")
        f.write("MIXED RANKINGS (ALL POSITIONS)\n")
        f.write(f"{'='*80}\n")
        
        # Sort by defense score (for non-GK players)
        defense_players = [p for p in valid_players if p.get("defense_score") is not None]
        write_ranking_table("🏆 TOP DEFENSE SCORES (ALL POSITIONS)", defense_players, "defense_score")
        
        # Sort by midfield score (for non-GK players)
        midfield_players = [p for p in valid_players if p.get("midfield_score") is not None]
        write_ranking_table("🎯 TOP MIDFIELD SCORES (ALL POSITIONS)", midfield_players, "midfield_score")
        
        # Sort by forward score (for non-GK players)
        forward_players = [p for p in valid_players if p.get("forward_score") is not None]
        write_ranking_table("⚽ TOP FORWARD SCORES (ALL POSITIONS)", forward_players, "forward_score")
        
        # ========== FINAL SCORE RANKING ==========
        f.write(f"\n{'='*80}\n")
        f.write("FINAL SCORE RANKING\n")
        f.write(f"{'='*80}\n")
        
        # Sort by final score (all players including GK)
        final_sorted = sorted(valid_players, key=lambda x: x.get("final_score", 0), reverse=True)
        f.write(f"\n⭐ TOP FINAL SCORES ({len(final_sorted)} players):\n")
        f.write(f"{'Rank':<6} {'Player':<30} {'Position':<8} {'Final Score':<15} {'GK Score':<15} {'Def':<8} {'Mid':<8} {'Fwd':<8}\n")
        f.write("-" * 80 + "\n")
        for i, player in enumerate(final_sorted[:15], 1):
            name = get_player_name(player)
            pos = player.get("normalized_position", "N/A")
            final_score = player.get("final_score", 0)
            gk_score = player.get("goalkeeper_score") if player.get("goalkeeper_score") is not None else "N/A"
            def_score = player.get("defense_score") if player.get("defense_score") is not None else "N/A"
            mid_score = player.get("midfield_score") if player.get("midfield_score") is not None else "N/A"
            fwd_score = player.get("forward_score") if player.get("forward_score") is not None else "N/A"
            
            gk_str = f"{gk_score:.2f}" if isinstance(gk_score, (int, float)) else str(gk_score)
            def_str = f"{def_score:.2f}" if isinstance(def_score, (int, float)) else str(def_score)
            mid_str = f"{mid_score:.2f}" if isinstance(mid_score, (int, float)) else str(mid_score)
            fwd_str = f"{fwd_score:.2f}" if isinstance(fwd_score, (int, float)) else str(fwd_score)
            
            f.write(f"{i:<6} {name:<30} {pos:<8} {final_score:<15.2f} {gk_str:<15} {def_str:<8} {mid_str:<8} {fwd_str:<8}\n")
        
        f.write(f"\n{'='*80}\n\n")
    
    print(f"Rankings saved to: {rankings_file}")

def clean_player_stats_data(
    match_details: Dict[str, Any],
    player_stats_raw: Dict[str, Any],
    match_id: Any,
) -> List[Dict[str, Any]]:
    """
    Build a list of rows for `player_match_stats` table, using:

      - lineup (home/away, starters+subs)
      - intercepted playerStats JSON (playerStats_raw)
    """

    home_team = match_details["content"]["lineup"]["homeTeam"]
    away_team = match_details["content"]["lineup"]["awayTeam"]

    home_players = home_team["starters"] + home_team["subs"]
    away_players = away_team["starters"] + away_team["subs"]

    # Get match scores from header
    header = match_details.get("header", {})
    teams = header.get("teams", [])
    home_team_score = teams[0].get("score", 0) if len(teams) > 0 else 0
    away_team_score = teams[1].get("score", 0) if len(teams) > 1 else 0
    
    # Calculate goals conceded for each team
    # Home team conceded = away team scored
    home_team_goals_conceded = away_team_score
    # Away team conceded = home team scored
    away_team_goals_conceded = home_team_score

    cleaned_rows: List[Dict[str, Any]] = []

    def process_players(players: List[Dict[str, Any]], team_id: int):
        for p in players:
            pid = p["id"]
            pid_str = str(pid)

            position_id = p.get("positionId")
            usual_pos_id = p.get("usualPlayingPositionId")
            raw_pos, norm_pos = _get_position_from_ids(position_id, usual_pos_id)

            raw_stats_entry = player_stats_raw.get(pid_str, {})
            
            # Try with integer key as fallback if string key not found
            if not raw_stats_entry:
                raw_stats_entry = player_stats_raw.get(pid, {})
            
            flat_stats = collect_player_stats(raw_stats_entry)

            # Ground & aerial duels
            gd_won, gd_total = extract_stat(flat_stats, "Ground duels won", fraction=True)
            ad_won, ad_total = extract_stat(flat_stats, "Aerial duels won", fraction=True)

            # Dribbles
            dribbles_completed, dribbles_attempted = extract_stat(
                flat_stats,
                "Successful dribbles",
                fraction=True,
            )

            # Passes
            passes_completed, passes_attempted = extract_stat(
                flat_stats,
                "Accurate passes",
                fraction=True,
            )

            # Crosses
            crosses_completed, crosses_attempted = extract_stat(
                flat_stats,
                "Accurate crosses",
                fraction=True,
            )

            # Long balls
            long_balls_completed, long_balls_attempted = extract_stat(
                flat_stats,
                "Accurate long balls",
                fraction=True,
            )

            row = {
                "match_id": match_id,
                "player_id": pid,
                "team_id": team_id,
                "first_name": p.get("firstName"),
                "last_name": p.get("lastName"),

                # Position
                "raw_position": raw_pos,
                "normalized_position": norm_pos,

                # Goalkeeper stats
                "saves": extract_stat(flat_stats, "Saves"),
                "goals_conceded": extract_stat(flat_stats, "Goals conceded"),
                "act_as_sweeper": extract_stat(flat_stats, "Sweeper (GK)"),
                "diving_save": extract_stat(flat_stats, "Diving save"),
                "high_claim": extract_stat(flat_stats, "High claim"),
                "saves_in_box": extract_stat(flat_stats, "Saves inside box"),
                "punches": extract_stat(flat_stats, "Punches"),
                "throws": extract_stat(flat_stats, "Throws"),

                # Defensive stats
                "tackles": extract_stat(flat_stats, "Tackles"),
                "last_man_tackles": extract_stat(flat_stats, "Last man tackle"),
                "blocks": extract_stat(flat_stats, "Blocks"),
                "clearances": extract_stat(flat_stats, "Clearances"),
                "headed_clearances": extract_stat(flat_stats, "Headed clearance"),
                "interceptions": extract_stat(flat_stats, "Interceptions"),
                "recoveries": extract_stat(flat_stats, "Recoveries"),
                "dribbled_past": extract_stat(flat_stats, "Dribbled past"),
                "fouls_committed": extract_stat(flat_stats, "Fouls committed"),
                "defensive_actions": extract_stat(flat_stats, "Defensive actions"),

                # Duels
                "ground_duels_completed": gd_won,
                "ground_duels_attempted": gd_total,
                "aerial_duels_completed": ad_won,
                "aerial_duels_attempted": ad_total,

                # Attacking stats
                "goals": extract_stat(flat_stats, "Goals"),
                "assists": extract_stat(flat_stats, "Assists"),
                "total_shots": extract_stat(flat_stats, "Total shots"),
                "shots_on_target": extract_stat(flat_stats, "Shots on target"),
                "touches": extract_stat(flat_stats, "Touches"),
                "touches_in_opp_box": extract_stat(flat_stats, "Touches in opposition box"),
                "dribbles_completed": dribbles_completed,
                "dribbles_attempted": dribbles_attempted,
                "passes_into_final_third": extract_stat(flat_stats, "Passes into final third"),
                "passes_completed": passes_completed,
                "passes_attempted": passes_attempted,
                "chances_created": extract_stat(flat_stats, "Chances created"),
                "penalties_won": extract_stat(flat_stats, "Penalties won"),
                "dispossesed": extract_stat(flat_stats, "Dispossessed"),
                "was_fouled": extract_stat(flat_stats, "Was fouled"),

                # Crosses
                "crosses_completed": crosses_completed,
                "crosses_attempted": crosses_attempted,

                # Long balls
                "long_balls_completed": long_balls_completed,
                "long_balls_attempted": long_balls_attempted,

                # Negative impact stats - try multiple label variations
                "own_goals": extract_stat_with_variants(
                    flat_stats, 
                    ["Own goals", "Own goal", "Own Goals"]
                ),
                "errors_leading_to_goal": extract_stat_with_variants(
                    flat_stats,
                    ["Error leading to goal", "Errors leading to goal", "Error leading to a goal", "Errors"]
                ),
            }

            
            if row["normalized_position"] == "GK":
                goalkeeper_score = calculate_player_goalkeeper_score(row)
                row["defense_score"] = None
                row["midfield_score"] = None
                row["forward_score"] = None
                row["goalkeeper_score"] = goalkeeper_score
                row["final_score"] = goalkeeper_score
            else:
                # Determine team goals conceded based on which team the player is on
                team_goals_conceded = home_team_goals_conceded if team_id == home_team["id"] else away_team_goals_conceded
                defense_score = calculate_player_defense_score(row, team_goals_conceded)
                midfield_score = calculate_player_midfield_score(row)
                forward_score = calculate_player_forward_score(row)
                final_score = calculate_final_score(row["normalized_position"], defense_score, midfield_score, forward_score)
                
                row["defense_score"] = defense_score
                row["midfield_score"] = midfield_score
                row["forward_score"] = forward_score
                row["goalkeeper_score"] = None
                row["final_score"] = final_score

            cleaned_rows.append(row)

    process_players(home_players, home_team["id"])
    process_players(away_players, away_team["id"])

    # Print sorted players by scores
    print_sorted_players_by_scores(cleaned_rows, match_id)

    return cleaned_rows
