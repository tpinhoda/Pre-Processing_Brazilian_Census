"""Generates processed data regarding election results"""
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from tqdm import tqdm
import pandas as pd
from src.data import Data

ID_COL_MAP = {
    "census tract": "[GEO]_ID_CENSUS_TRACT",
    "neighborhood": "[GEO]_ID_NEIGHBORHOOD",
    "subdistrict": "[GEO]_ID_SUBDISTRICT",
    "district": "[GEO]_ID_DISTRICT",
    "city": "[GEO]_ID_CITY",
    "micro region": "[GEO]_ID_MICRO_REGION",
    "meso region": "[GEO]_ID_MESO_REGION",
    "uf": "[GEO]_ID_UF",
}

@dataclass
class Processed(Data):
    """Represents the Brazilian census results in processed state of processing.

    This object pre-processes the Brazilian census results.

    Attributes
    ----------
        aggregation_level: str
            The data geogrephical level of aggrevation
    """

    aggregation_level: str = None
    na_threshold: float = None
    __processed_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    
    def _drop_duplicated_row_from_merge(self):
        delete_cols = [c for c in self.__processed_data.columns if "DELETE" in c]
        self.__processed_data.drop(delete_cols, axis=1, inplace=True)
    
    def _merge_data(self):
        interim_path =self._get_data_name_folders_path("interim")
        interim_agg_path = os.path.join(interim_path, self.aggregation_level)
        filenames = self._get_files_in_dir(interim_agg_path)
        for filename in tqdm(filenames, desc="Merging data", leave=False):
            interim_data = pd.read_csv(os.path.join(interim_agg_path, filename), low_memory=False)
            if not self.__processed_data.empty:
                self.__processed_data = self.__processed_data.merge(interim_data, on=ID_COL_MAP[self.aggregation_level], how="outer", suffixes=("", "DELETE"))
            else:
                self.__processed_data = interim_data.copy()
        self._drop_duplicated_row_from_merge()
    
    def _drop_cols_rows_na_all(self):
        """Drop rows and columns with 100 NA values from raw data"""
        threshold_na_row = int(self.na_threshold * len(self.__processed_data.columns) /100)
        threshold_na_col = int(self.na_threshold * len(self.__processed_data) /100)
        self.__processed_data.dropna(how="all", axis=0, thresh=threshold_na_row, inplace=True)
        self.__processed_data.dropna(how="all", axis=1, thresh=threshold_na_col, inplace=True)
    
    def _fill_na(self):
        self.__processed_data.fillna(0, inplace=True)
        
    def run(self):
        """Run processed process"""
        self.init_logger_name(msg="Census (Processed)")
        self.init_state(state="processed")
        self.logger_info("Generating processed data.")
        self._make_folders(
            folders=[self.data_name, self.aggregation_level]
        )
        self._merge_data()
        self._drop_cols_rows_na_all()
        
        
