"""Generates interim results data"""
import os
import itertools
from typing import List, Optional
from dataclasses import dataclass, field
from tqdm import tqdm
import pandas as pd
from src.data import Data


PREFIX_COL_CENSUS = "[CENSUS]"
ENCODINGS = {"default": "mbcs", "encode_1": "utf-8", "encode_2": "latin"}
SEPS = {"default": ";", "sep_1": ","}
GEO_COLUMNS = {
    "Cod_setor": "[GEO]_ID_CENSUS_TRACT",
    "Cod_Grandes Regiões": "[GEO]_ID_REGION",
    "Cod_Grandes Regiäes": "[GEO]_ID_REGION",  # There is a typo in TO -> BASICO.csv
    "Nome_Grande_Regiao": "[GEO]_REGION",
    "Cod_UF": "[GEO]_ID_UF",
    "Nome_da_UF ": "[GEO]_UF",
    "Cod_meso": "[GEO]_ID_MESO_REGION",
    "Nome_da_meso": "[GEO]_MESO_REGION",
    "Cod_micro": "[GEO]_ID_MICRO_REGION",
    "Nome_da_micro": "[GEO]_MICRO_REGION",
    "Cod_RM": "[GEO]_ID_RM",
    "Nome_da_RM": "[GEO]_RM",
    "Cod_municipio": "[GEO]_ID_CITY",
    "Nome_do_municipio": "[GEO]_CITY",
    "Cod_distrito": "[GEO]_ID_DISTRICT",
    "Nome_do_distrito": "[GEO]_DISTRICT",
    "Cod_subdistrito": "[GEO]_ID_SUBDISTRICT",
    "Nome_do_subdistrito": "[GEO]_SUBDISTRICT",
    "Cod_bairro": "[GEO]_ID_NEIGHBORHOOD",
    "Nome_do_bairro": "[GEO]_NEIGHBORHOOD",
}

AGGR_COL_MAP = {
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
class Interim(Data):
    """Represents the Brazilian election results in interim state of processing.

    This object pre-processes the Brazilian election results.

    Attributes
    ----------
        aggregation_level: str
            The data geogrephical level of aggrevation
        ref_file: str
            The name of the reference file (Basico.csv)
        id_col: str
            The id column
        char_col_census: str
            The character to identify census columns
        char_na_values: str
            The character used as NA
        char_decimal: str
            The decimal separator
    """

    aggregation_level: str = None
    ref_file: str = None
    id_col: str = None
    char_col_census: str = None
    char_na_values: str = None
    char_decimal: str = None
    __raw_data_path: Optional[str] = None
    __list_filenames: Optional[str] = None
    __ref_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    __raw_data: pd.DataFrame = field(default_factory=pd.DataFrame)

    def _init_raw_data_path(self):
        """Initialize the raw path attribute"""
        self.__raw_data_path = self._get_data_name_folders_path(state="raw")

    def _init_list_filename(self, filenames: List[str]):
        """Initialize the list of filenames"""
        if not self.__list_filenames:
            self.__list_filenames = filenames

    @staticmethod
    def _check_wrong_encoding(data, keys):
        """Check if the data was read with the correct encoding"""
        try:
            if len(keys) > 1:  # If not basico.csv
                if (
                    "Cod_Grandes Regiäes" in data.columns
                ):  # Theres is a typo in TO -> BASICO.CSV
                    keys.remove("Cod_Grandes Regiões")
                else:
                    keys.remove("Cod_Grandes Regiäes")

            data[keys]
            return True
        except KeyError:
            return False

    def _read_data_csv(self, filename: str, encoding: str, sep: str):
        """Read a csv file using pandas"""
        return pd.read_csv(
            filename,
            sep=sep,
            encoding=encoding,
            decimal=self.char_decimal,
            na_values=self.char_na_values,
            error_bad_lines=False,
            warn_bad_lines=False,
            low_memory=False,
        ).infer_objects()

    @staticmethod
    def _get_col_by_tag(df, tag: str):
        """Get columns from a given tag"""
        return [c for c in df.columns if tag in c]

    def _correct_id_uf_cols(self):
        most_frequent = self.__ref_data["[GEO]_ID_UF"].value_counts().max()
        self.__ref_data["[GEO]_ID_UF"] = [most_frequent] * len(self.__ref_data)

    def _read_ref_data(self, folder_path: str):
        """Read the reference file"""
        for encoding in ENCODINGS.values():
            self.__ref_data = self._read_data_csv(
                filename=os.path.join(folder_path, self.ref_file),
                encoding=encoding,
                sep=SEPS["default"],
            )
            if self._check_wrong_encoding(self.__ref_data, list(GEO_COLUMNS.keys())):
                break
        # Rename columns to standardize
        self.__ref_data.rename(columns=GEO_COLUMNS, inplace=True)
        # Filter to only geo cols
        geo_cols = self._get_col_by_tag(df=self.__ref_data, tag="GEO")
        self.__ref_data = self.__ref_data[geo_cols]
        self._correct_id_uf_cols()  # There are some id uf with wrong inputation

    def _read_raw_data(self, filename: str) -> pd.DataFrame:
        """Read the raw data"""
        for sep, enc in itertools.product(*[SEPS.values(), ENCODINGS.values()]):
            self.__raw_data = self._read_data_csv(filename, sep=sep, encoding=enc)
            if len(self.__raw_data.columns) > 1 and self._check_wrong_encoding(
                self.__raw_data, [self.id_col]
            ):
                break

    def _drop_unecessary_cols(self):
        """Drop unecessary columns from raw data"""
        drop_cols = [
            c
            for c in self.__raw_data.columns
            if self.char_col_census not in c and c != self.id_col
        ]
        unnamed_cols = [c for c in self.__raw_data.columns if "^Unnamed" in c]
        self.__raw_data.drop(drop_cols + unnamed_cols, axis=1, inplace=True)

    def _drop_cols_rows_na_all(self):
        """Drop rows and columns with 100 NA values from raw data"""
        self.__raw_data.dropna(how="all", axis=0, inplace=True)
        self.__raw_data.dropna(how="all", axis=1, inplace=True)

    def _fill_na(self):
        """Fill the NA with 0 in the raw data"""
        self.__raw_data.fillna(0, inplace=True)

    def _rename_census_cols(self, filename: str):
        """Assign tag CENSUS to columns"""
        new_cols = {
            c: f"{PREFIX_COL_CENSUS}_{filename}_{c}"
            for c in self.__raw_data.columns
            if c != self.id_col
        }
        self.__raw_data.rename(columns={**new_cols, **GEO_COLUMNS}, inplace=True)

    def _convert_census_cols(self):
        """There are some string values in census col that need to be set to nan, infer_objects does not get"""
        self.__raw_data = self.__raw_data.apply(
            lambda col: pd.to_numeric(col, errors="coerce"), axis=1
        )

    def _add_geo_cols(self):
        """Merge ref data with raw data"""
        self.__raw_data = self.__ref_data.merge(
            self.__raw_data, on=GEO_COLUMNS[self.id_col], how="left"
        )

    def _create_aggregate_map(self):
        """Create aggregate map"""
        geo_cols = {c: "first" for c in self.__raw_data.columns if "[GEO]" in c}
        census_cols = {c: "sum" for c in self.__raw_data.columns if "[CENSUS]" in c}
        return {**geo_cols, **census_cols}

    def _get_aggr_col(self):
        """Returns aggregation column"""
        return AGGR_COL_MAP[self.aggregation_level]

    def _aggregate_data(self):
        """Aggregate the raw data by aggregation level"""
        agg_map = self._create_aggregate_map()
        aggr_col = self._get_aggr_col()
        self.__raw_data = self.__raw_data.groupby(by=aggr_col, as_index=False).agg(
            func=agg_map
        )

    def _save_data(self, filename: str):
        """Save raw data"""
        self.__raw_data.to_csv(os.path.join(self.cur_dir, filename), index=False)

    def _preprocessing(self):
        """Preprocess raw data"""
        self.logger_info("Pre-processing data.")
        folders = self._get_folders_in_dir(directory=self.__raw_data_path)
        for folder in tqdm(folders, desc="Pre-Processing", leave=False):
            self._mkdir(folder)  # Create interim folder
            folder_path = os.path.join(self.__raw_data_path, folder)
            self._read_ref_data(folder_path=folder_path)
            filenames = self._get_files_in_dir(folder_path)
            self._init_list_filename(filenames)
            # Associate the codes to each file in filenames
            for filename in filenames:
                # Load raw data
                filepath = os.path.join(folder_path, filename)
                self._read_raw_data(filename=filepath)
                self._drop_unecessary_cols()
                self._drop_cols_rows_na_all()
                self._convert_census_cols()
                self._fill_na()
                self._rename_census_cols(filename=filename.split(".")[0])
                self._add_geo_cols()
                self._aggregate_data()
                self._save_data(filename)
            self.cur_dir = os.path.join(
                self._get_state_folders_path("interim"),
                self.data_name,
                self.aggregation_level,
            )

    def concat_data(self):
        """Concatenate census by file name"""
        folders = self._get_folders_in_dir(directory=self.__raw_data_path)
        for filename in self.__list_filenames:
            df_list = []
            self.logger_info(f"Concatenating files: {filename}")
            for folder in tqdm(folders, desc="Concatenating", leave=False):
                folder_path = os.path.join(self.cur_dir, folder)
                data = pd.read_csv(
                    os.path.join(folder_path, filename), encoding="utf-8"
                )
                df_list.append(data)
            concat_df = pd.concat(df_list, axis=0)
            # Save the concatenated data
            concat_df.to_csv(os.path.join(self.cur_dir, filename), index=False)
        self._remove_folders_from_cur_dir()

    def run(self):
        """Run interim process"""
        self.init_logger_name(msg="Census (Interim)")
        self.init_state(state="interim")
        self.logger_info("Generating interim data.")
        self._make_folders(folders=[self.data_name, self.aggregation_level])
        self._init_raw_data_path()
        self._preprocessing()
        self.concat_data()
