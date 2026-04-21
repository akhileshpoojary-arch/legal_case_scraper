import asyncio
import logging
from typing import Any
from scrapers.base import BaseEjagritiExtractor, EJAGRITI_ALL_STATES, EJAGRITI_HEADERS

logger = logging.getLogger("legal_scraper.dcdrc.extractor")

class DCDRCExtractor(BaseEjagritiExtractor):
    SOURCE = "DCDRC"
    TYPE_ID = "3"
    
    def __init__(self, session_manager):
        super().__init__(session_manager)
        # Districts will be populated by fetch_districts
        self.courts = []
        
    async def fetch_districts(self):
        """Asynchronously pre-fetches all district commission IDs for all 36 states."""
        logger.info("DCDRC: Pre-fetching district commissions for all states...")
        
        async def fetch_one(state):
            sid = str(state["commissionId"])
            url = f"{self.BASE_URL}/getDistrictCommissionByCommissionId"
            try:
                resp = await self.sm.get(url, params={"commissionId": sid}, timeout=20)
                dists = self._extract_list(resp)
                return state, dists
            except Exception as e:
                logger.error(f"DCDRC: Failed to fetch districts for {state['commissionNameEn']}: {e}")
                return state, []

        coros = [fetch_one(s) for s in EJAGRITI_ALL_STATES]
        results = await asyncio.gather(*coros)
        
        for state, dists in results:
            sname = state["commissionNameEn"]
            for dist in dists:
                did = dist.get("commissionId") or dist.get("districtCommissionId") or dist.get("id") or dist.get("districtId")
                dname = dist.get("commissionNameEn") or dist.get("districtNameEn") or dist.get("districtName") or dist.get("name") or str(did)
                if did:
                    self.courts.append({
                        "id": str(did),
                        "name": f"DCDRC - {dname} ({sname})",
                        "type_id": "3",
                        "level": "DCDRC"
                    })
                    
        logger.info(f"DCDRC: Successfully loaded {len(self.courts)} district commissions.")
