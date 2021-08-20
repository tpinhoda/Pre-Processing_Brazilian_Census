"""Generates processed data regarding census results"""
import os
from dataclasses import dataclass, field
from typing import List
from tqdm import tqdm
import pandas as pd
from src.data import Data

DOMICILE_TAGS = ["DOMICILIO01", "ENTORNO01", "ENTORNO02"]

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

TOTAL_COLS = {
    "person": "[CENSUS]_DOMICILIO02_V002",
    "domicile": "[CENSUS]_DOMICILIO01_V001",
    "income_person": "[CENSUS]_PESSOARENDA_V022",
    "income_domicile": "[CENSUS]_DOMICILIORENDA_V002",
}


@dataclass
class Processed(Data):
    """Represents the Brazilian census results in processed state of processing.

    This object pre-processes the Brazilian census results.

    Attributes
    ----------
        aggregation_level: str
            The data geogrephical level of aggrevation
        na_threshold: float
            Non_NA threshold to drop column
        global_cols: int
            Whether to include global features [0, 1]
        global_threshold: float
            Threshold to remove global features
    """

    aggregation_level: str = None
    na_threshold: float = None
    global_cols: int = None
    global_threshold: float = None
    __processed_data: pd.DataFrame = field(default_factory=pd.DataFrame)

    def _drop_duplicated_col_from_merge(self):
        """Remove duplicated cols from merged data"""
        delete_cols = [c for c in self.__processed_data.columns if "DELETE" in c]
        self.__processed_data.drop(delete_cols, axis=1, inplace=True)

    def _merge_data(self):
        """Merge data from the same filename"""
        self.logger_info("Merging into a single dataset.")
        interim_path = self._get_data_name_folders_path("interim")
        interim_agg_path = os.path.join(interim_path, self.aggregation_level)
        filenames = self._get_files_in_dir(interim_agg_path)
        for filename in tqdm(filenames, desc="Merging data", leave=False):
            interim_data = pd.read_csv(
                os.path.join(interim_agg_path, filename), low_memory=False
            ).infer_objects()
            if not self.__processed_data.empty:
                self.__processed_data = self.__processed_data.merge(
                    interim_data,
                    on=ID_COL_MAP[self.aggregation_level],
                    how="outer",
                    suffixes=("", "DELETE"),
                )
            else:
                self.__processed_data = interim_data.copy()
        self._drop_duplicated_col_from_merge()

    def _drop_cols_rows_na_all(self):
        """Drop rows and columns with threshold% NA values from raw data"""
        threshold_na_row = int(
            self.na_threshold * len(self.__processed_data.columns) / 100
        )
        threshold_na_col = int(self.na_threshold * len(self.__processed_data) / 100)
        self.__processed_data.dropna(
            how="all", axis=0, thresh=threshold_na_row, inplace=True
        )
        self.__processed_data.dropna(
            how="all", axis=1, thresh=threshold_na_col, inplace=True
        )

    def _fill_na(self):
        """Fill na values"""
        self.__processed_data.fillna(0, inplace=True)

    def _get_col_by_tag(self, tag: str):
        """Get columns by tag name"""
        return [c for c in self.__processed_data.columns if tag in c]

    def _check_income_col(self, col):
        """Check if a column describe income"""
        greater_population = (
            self.__processed_data["[CENSUS]_DOMICILIO02_V001"]
            < self.__processed_data[col]
        ).any()
        greater_domicilies = (
            self.__processed_data["[CENSUS]_DOMICILIO01_V001"]
            < self.__processed_data[col]
        ).any()
        return greater_population & greater_domicilies

    def _get_income_cols(self):
        """Return income related cols"""
        census_col = [c for c in self.__processed_data.columns if "[CENSUS]" in c]
        return [
            col
            for col in census_col
            if self._check_income_col(col) and "BASICO" not in col
        ]

    def _get_domicile_cols(self):
        """Return domicile related cols"""
        return [
            c
            for c in self.__processed_data.columns
            if any(tag in c for tag in DOMICILE_TAGS)
        ]

    def _separate_cols_by_description(self):
        """Separate columns by groups"""
        basic = self._get_col_by_tag(tag="BASICO")
        geo = self._get_col_by_tag(tag="GEO")
        income = self._get_income_cols()
        income_person = [
            c for c in income if any(tag in c for tag in ["PESSOA", "RESPONSAVEL"])
        ]
        income_domicilie = [c for c in income if "DOMICILIO" in c]
        domicile = self._get_domicile_cols()
        person = [
            c
            for c in self.__processed_data.columns
            if c not in income + domicile + basic + geo
        ]

        return {
            "income_person": income_person,
            "income_domicile": income_domicilie,
            "domicile": domicile,
            "person": person,
        }

    def _normalize_by_total(self, cols: List[str], total_col: str):
        """Normalize data by total"""
        self.__processed_data[cols] = self.__processed_data[cols].divide(
            self.__processed_data[total_col], axis=0
        )

    def _normalize_by_mim_max(self, cols: List[str]):
        """Normalize data using max min"""
        self.__processed_data[cols] = (
            self.__processed_data[cols] - self.__processed_data[cols].min()
        ) / (self.__processed_data[cols].max() - self.__processed_data[cols].min())

    def _normalize_data(self):
        """Normalize processed data"""
        self.logger_info("Normalizing data.")
        cols_separetated = self._separate_cols_by_description()
        for key, cols in cols_separetated.items():
            self._normalize_by_total(cols=cols, total_col=TOTAL_COLS[key])
        min_max_cols = self._get_col_by_tag(tag="BASICO") + list(TOTAL_COLS.values())
        if self.global_cols:
            min_max_cols = self._get_col_by_tag(tag="BASICO") + list(
                TOTAL_COLS.values()
            )
            self._normalize_by_mim_max(cols=min_max_cols)
        else:
            self.__processed_data.drop(min_max_cols, axis=1, inplace=True)
            self._remove_global_cols()

    def _remove_global_cols(self):
        """Remove global features"""
        census_col = self._get_col_by_tag(tag="[CENSUS]")
        global_cols = [
            c
            for c in census_col
            if all(self.__processed_data[c] > self.global_threshold)
        ]
        self.__processed_data.drop(global_cols, axis=1, inplace=True)

    def _remove_duplicated_cols(self):
        """Remove duplicated columns"""
        dupli = self.__processed_data.T.duplicated(keep="first")
        dupli_cols = self.__processed_data.T.loc[dupli].index.values
        self.__processed_data.drop(dupli_cols, axis=1, inplace=True)

    def _save_data(self):
        """Save processed data"""
        self.logger_info("Saving final data.")
        if self.global_cols:
            self.__processed_data.to_csv(
                os.path.join(self.cur_dir, "data_with_global.csv"), index=False
            )
        else:
            self.__processed_data.to_csv(
                os.path.join(self.cur_dir, "data_no_global.csv"), index=False
            )

    def run(self):
        """Run processed process"""
        self.init_logger_name(msg="Census (Processed)")
        self.init_state(state="processed")
        self.logger_info("Generating processed data.")
        self._make_folders(folders=[self.data_name, self.aggregation_level])
        self._merge_data()
        self._drop_cols_rows_na_all()
        self.__processed_data = self.__processed_data.convert_dtypes()
        self._normalize_data()
        self._remove_duplicated_cols()
        self._save_data()
