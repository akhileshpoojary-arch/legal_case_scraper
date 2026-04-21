from scrapers.base import BaseEjagritiExtractor, EJAGRITI_ALL_STATES

class SCDRCExtractor(BaseEjagritiExtractor):
    SOURCE = "SCDRC"
    TYPE_ID = "2"
    
    def __init__(self, session_manager):
        super().__init__(session_manager)
        self.courts = [
            {"id": str(s["commissionId"]), "name": f"SCDRC - {s['commissionNameEn']}", "type_id": "2", "level": "SCDRC"}
            for s in EJAGRITI_ALL_STATES
        ]
