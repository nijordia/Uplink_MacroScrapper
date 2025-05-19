from typing import List
from entities.economic_data import EconomicData
from interfaces.database.economic_data_repository import EconomicDataRepository

class GenerateIndicatorTableUseCase:
    def __init__(self, repository: EconomicDataRepository):
        self.repository = repository

    def execute(self, country_code: str, keyword: str) -> List[EconomicData]:
        return self.repository.get_latest_by_indicator_name(country_code, keyword)