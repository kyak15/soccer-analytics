import sys, os
import asyncio
import json
from scraper.extract.get_match_urls import (get_match_urls_from_start_to_current_round)
from scraper.extract.scrape_one_match import (scrape_match_fast,save_match)
from scraper.transform.transform_full_match import (transform_full_match)
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

RAW_DIR = "scraper/extract/raw"
TRANSFORMED_DIR = "scraper/transform/ready"

async def run_pipeline_backfill(start_round=0, end_round=10):
    '''
    Function that retrieves the first 11 weeks of EPL fixtures to populate the database with pre-existing match, player, team, and stat data.
    '''

    match_urls = await get_match_urls_from_start_to_current_round(
        start_round=start_round,
        end_round=end_round
    )

    print(f"{len(match_urls)} total match URLs.")

    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(TRANSFORMED_DIR, exist_ok=True)

    # save raw json from each match
    for url in match_urls:
        print(f"Extracting match: {url}")
        match_id = url.split("#")[1].split(":")[0]
        raw_path = os.path.join(RAW_DIR, f"{match_id}.json")
        transformed_path = os.path.join(TRANSFORMED_DIR, f"{match_id}.json")

        # Skip if already scraped
        if os.path.exists(raw_path):
            print(f"Raw exists: {match_id}.json")
            # (Skip extraction but still transform+load)
        else:
            # Scrape raw data
            try:
                raw_data = await scrape_match_fast(url)
            except Exception as e:
                print(f" ERROR SCRAPING {url}: {e}")
                continue

            save_match(raw_data)  # saves into RAW_DIR
            print(f"Saved raw: {raw_path}")
        
        
        # Load raw json for data transformation
        with open(raw_path, "r") as f:
            raw_json = json.load(f)

        transformed = transform_full_match(raw_json)

        with open(transformed_path, "w") as f:
            json.dump(transformed, f, indent=2)

        print(f"SAVED TRANSFORMED: {transformed_path}")

        # TODO: Work on database insertion step 

    print("\nPipeline Complete")


if __name__ == "__main__":
    asyncio.run(run_pipeline_backfill(0, 10))
