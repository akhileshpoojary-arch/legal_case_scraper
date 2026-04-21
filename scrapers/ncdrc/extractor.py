from scrapers.base import BaseEjagritiExtractor

class NCDRCExtractor(BaseEjagritiExtractor):
    SOURCE = "NCDRC"
    TYPE_ID = "1"
    
    def __init__(self, session_manager):
        super().__init__(session_manager)
        self.courts = [{"id": "11000000", "name": "NCDRC - NATIONAL", "type_id": "1", "level": "NCDRC"}]
