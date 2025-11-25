import asyncio
import re
from playwright.async_api import async_playwright
from scraper.config import FOTMOB_LEAGUE_FIXTURES_URL, FOTMOB_BASE_URL

async def get_match_urls_from_start_to_current_round(start_round: int, end_round: int):
    all_urls = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for rnd in range(start_round, end_round + 1):
            url = FOTMOB_LEAGUE_FIXTURES_URL + str(rnd)
            print(f"\n=== ROUND {rnd} ===")
            print(f"Visiting: {url}")

            await page.goto(url, wait_until="networkidle")
            await page.wait_for_timeout(2000)

            # Targets the exact structure: <a data-testid="livescores-match" class="...MatchWrapper...">
            rows = page.locator("a[data-testid='livescores-match']")
            count = await rows.count()
            print(f"Found {count} match rows")

            for i in range(count):
                row = rows.nth(i)

                # Get all text content from the row to check for match status
                row_text = await row.inner_text()
                
                # Extract href first to help with debugging
                href = await row.get_attribute("href")
                if not href:
                    continue
                
                row_text_upper = row_text.upper()
                
                # Check for "FT" first - this is the definitive indicator of completed matches
                # "FT" appearing anywhere means the match is finished
                # Note: Team names like "Liverpool" contain "LIVE" and "Brighton & Hove Albion" contains "HT"
                # So we prioritize "FT" as the definitive status indicator
                has_ft = "FT" in row_text_upper
                
                # If FT is present, it's definitely completed (even if team names contain "LIVE" or "HT")
                if has_ft:
                    # Match is completed - proceed to extract URL
                    pass
                else:
                    # No FT - check for other indicators to determine status
                    # Check for time patterns (e.g., "14:00", "15:30") which indicate scheduled matches
                    has_time_pattern = bool(re.search(r'\d{1,2}:\d{2}', row_text))
                    
                    # Check for LIVE/HT as standalone status (not in team names)
                    # Use word boundaries to avoid false positives from team names
                    has_live_status = bool(re.search(r'\bLIVE\b', row_text_upper))
                    has_ht_status = bool(re.search(r'\bHT\b', row_text_upper))
                    
                    if has_live_status or has_ht_status:
                        # Match is live or at half time - skip
                        print(f"  ⏭ Skipped (LIVE/HT): {href[:50]}...")
                        continue
                    
                    if has_time_pattern:
                        # Has time but no FT - scheduled match
                        print(f"  ⏭ Skipped (scheduled): {href[:50]}...")
                        continue
                    
                    # Check for score pattern (e.g., "2-1", "0-0")
                    has_score_pattern = bool(re.search(r'\d+\s*-\s*\d+', row_text))
                    if has_score_pattern:
                        # Has score but no FT and no time/LIVE/HT - likely completed
                        pass
                    else:
                        # Can't determine if completed - skip
                        print(f"  ⏭ Skipped (unknown status): {href[:50]}... | Text: {row_text[:100]}")
                        continue

                # Build full URL
                if href.startswith("http"):
                    full = href
                else:
                    full = FOTMOB_BASE_URL + href

                # Extract match ID from the URL (the part after #)
                if "#" in href:
                    match_id = href.split("#")[1]
                else:
                    # Fallback: try to extract from full URL
                    parts = full.split("#")
                    if len(parts) > 1:
                        match_id = parts[1]
                    else:
                        # Last resort: extract from path
                        match_id = full.split("/")[-1]

                # Force lineup tab
                base_url = full.split("#")[0]
                final = f"{base_url}#{match_id}:tab=lineup"
                all_urls.add(final)
                print(f"Completed: {final}")

            print(f"Completed matches so far: {len(all_urls)}")

        await browser.close()

    all_urls = sorted(all_urls)
    print("\n======================================")
    print(f"TOTAL COMPLETED MATCH URLS: {len(all_urls)}")
    for u in all_urls:
        print(u)

    return all_urls


if __name__ == "__main__":
    asyncio.run(get_match_urls_from_start_to_current_round(0, 38))
