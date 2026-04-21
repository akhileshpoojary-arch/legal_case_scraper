import json
import logging
from utils.name_filter import filter_individual_matches
import asyncio
from daily_run.sheet_search import SCSheetScraper
import config

logging.basicConfig(level=logging.INFO)

async def main():
    config.ENTITY_TYPE = "individual"
    
    scraper = SCSheetScraper()
    rows = await scraper.run("RANJIT KESHARI DAS")
    print(f"Found {len(rows)} matching rows before filter.")
    
    kept = filter_individual_matches(rows, "RANJIT KESHARI DAS", "individual")
    print(f"Kept {len(kept)} rows.")
    
    for r in rows:
        if r not in kept:
            print("Dropped row:")
            for k in ["respondent", "otherRespondent", "petitioner", "otherPetitioner", "caseNumber", "partyName"]:
                val = r.get(k, '')
                print(f"  {k}: {repr(val)} (len: {len(val) if val else 0})")

if __name__ == "__main__":
    asyncio.run(main())
