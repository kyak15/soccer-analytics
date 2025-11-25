import asyncio
from playwright.async_api import async_playwright
import json
import re
import os
import unicodedata

RAW_DIR = "scraper/extract/raw"   # where to save match files


def clean_name(name: str):
    name = name.lower()
    name = unicodedata.normalize("NFKD", name)
    name = re.sub(r"[^a-z0-9]+", "-", name)
    return name.strip("-")


async def scrape_match_fast(match_url: str):
    match_id = match_url.split("#")[1].split(":")[0]

    match_details_json = {"data": None}
    player_stats_json = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        async def handle_response(res):
            url = res.url

            # match details specific
            if "/matchDetails?" in url and f"matchId={match_id}" in url:
                try:
                    match_details_json["data"] = await res.json()
                    print("Captured matchDetails")
                except:
                    print("Could not parse matchDetails JSON")

            
            # player stats (getting all endpoints with playerId)
            if "playerId=" in url and f"matchId={match_id}" in url:
                try:
                    data = await res.json()
                    pid = re.search(r"playerId=(\d+)", url)
                    if pid:
                        pid_val = pid.group(1)
                        player_stats_json[pid_val] = data
                        print(f"Captured stats for player {pid_val}")
                except:
                    print(f"Failed to parse player stats for {url}")

        page.on("response", handle_response)

        print("Loading page...")
        await page.goto(match_url, wait_until="networkidle")

        # Ensure lineup is loaded
        await page.get_by_role("button", name="Lineup").click()
        await page.wait_for_timeout(2000)

        await browser.close()

    md = match_details_json["data"]

    if (
        md is None
        or (isinstance(md, dict) and md.get("error"))
    ):
        raise Exception(f"FotMob returned no matchDetails for match {match_id}")

    return {
        "matchId": match_id,
        "matchDetails": match_details_json["data"],
        "playerStats": player_stats_json,
    }


def save_match(raw_data):
    """
    Save a scraped match to RAW_DIR using only the match_id.
    File format: 4813475.json
    """
    # Extract matchId
    match_id = raw_data["matchId"] 

    # Build file path
    filename = f"{match_id}.json"
    path = os.path.join(RAW_DIR, filename)

    # Ensure directory exists
    os.makedirs(RAW_DIR, exist_ok=True)

    # Save JSON file
    with open(path, "w") as f:
        json.dump(raw_data, f, indent=2)

    print(f"Saved raw match → {path}")


async def main():
    url = "https://www.fotmob.com/matches/manchester-city-vs-liverpool/2f48yd#4813480"
    data = await scrape_match_fast(url)
    save_match(data)


if __name__ == "__main__":
    asyncio.run(main())
