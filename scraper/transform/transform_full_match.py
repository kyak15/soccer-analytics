from typing import Dict, Any, List, Tuple, Optional

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

    107: ("LW", "FW"),
    87:  ("LW", "FW"),

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
    115: ("ST", "FW"),
    106: ("ST", "FW"),
    104: ("ST", "FW"),
    105: ("ST", "FW"),
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

    raw_pos, norm_pos = POSITION_ID_MAP.get(position_id, ("UNK", "UNK"))

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

    cleaned_rows: List[Dict[str, Any]] = []

    def process_players(players: List[Dict[str, Any]], team_id: int):
        for p in players:
            pid = p["id"]
            pid_str = str(pid)

            position_id = p.get("positionId")
            raw_pos, norm_pos = POSITION_ID_MAP.get(position_id, ("UNK", "UNK"))

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
            }

            cleaned_rows.append(row)

    process_players(home_players, home_team["id"])
    process_players(away_players, away_team["id"])

    return cleaned_rows
