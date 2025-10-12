import abc
from typing import List

from pydantic import BaseModel, ConfigDict, RootModel


class Feature(BaseModel):
    name: str
    pandas_dtype: str
    tags: List[str] = []


class FeatureSet(RootModel, abc.ABC):
    root: List[Feature]
    model_config = ConfigDict(frozen=True)

    def __iter__(self):
        return iter(self.root)

    def get_pandas_dtypes(self):
        return {feature.name: feature.pandas_dtype for feature in self.root}

    def get_feature_names_by_tag(self, tag: str) -> List[str]:
        return [feature.name for feature in self.root if tag in feature.tags]

    def get_feature_names_by_any_tags_and_dtype(
        self, any_tags: List[str], dtype: str
    ) -> List[str]:
        """
        Returns feature names that have any tags.
        """
        feature_names = []
        for feature in self.root:
            if dtype == feature.pandas_dtype:
                for tag in any_tags:
                    if tag in feature.tags:
                        feature_names.append(feature.name)
                        break
        return feature_names

    @abc.abstractmethod
    def get_label(self) -> str:
        pass
