from typing import List

from JapanHorseRaceAnalytics.models.base import Feature, FeatureSet


def get_features() -> List[Feature]:
    return []


class _FeatureSet(FeatureSet):
    def get_label(self) -> str:
        return "複勝的中"


Features = _FeatureSet(get_features())
