from typing import List

import yaml

from jhra.models.base import Feature, FeatureSet


def get_features() -> List[Feature]:
    with open("models/curated/_models.yml", "r") as f:
        models = yaml.safe_load(f)

    # find model where name = features_20240202_v1
    model = None
    for m in models["models"]:
        if m["name"] == "features_20240202_v1":
            model = m
            break

    if model is None:
        raise ValueError("Model not found")

    result = []
    for column in model["columns"]:
        result.append(
            Feature(
                name=column["name"],
                pandas_dtype=column["meta"]["pandas_dtype"],
                tags=column.get("tags", []),
            )
        )
    return result


class _FeatureSet(FeatureSet):
    def get_label(self) -> str:
        return "先読み注意_複勝的中"


Features = _FeatureSet(get_features())
