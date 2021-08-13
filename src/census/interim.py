"""Generates interim results data"""
from os.path import join
from typing import Dict, List
from dataclasses import dataclass, field
from tqdm import tqdm
import pandas as pd
from pandas.api.types import is_numeric_dtype
from pandas_profiling import ProfileReport
from src.data import Data


MAP_CANDIDACY = {"president": 1, "governor": 3}

MAP_BLANK_NULL = {"NULL": 96, "BLANK": 95}

UNIQUE_ID = [
    "[GEO]_ID_TSE_CITY",
    "[GEO]_ID_POLLING_ZONE",
    "[GEO]_ID_POLLING_PLACE",
    "[GEO]_ID_POLLING_SECTION",
]

MAP_COL_RENAME = {
    "SG_ UF": "[GEO]_UF",
    "CD_MUNICIPIO": "[GEO]_ID_TSE_CITY",
    "NM_MUNICIPIO": "[GEO]_CITY",
    "NR_ZONA": "[GEO]_ID_POLLING_ZONE",
    "NR_SECAO": "[GEO]_ID_POLLING_SECTION",
    "NR_LOCAL_VOTACAO": "[GEO]_ID_POLLING_PLACE",
    "CD_CARGO_PERGUNTA": "[ELECTION]_ID_CANDIDACY_POSITION",
    "QT_APTOS": "[ELECTION]_ELECTORATE",
    "QT_COMPARECIMENTO": "[ELECTION]_TURNOUT",
    "QT_ABSTENCOES": "[ELECTION]_ABSTENTIONS",
    "NR_VOTAVEL": "[ELECTION]_ID_CANDIDATE",
    "QT_VOTOS": "[ELECTION]_VOTES",
    "QT_ELEITORES_BIOMETRIA_NH": "[ELECTION]_ELECTORATE_BIOMETRIA",
}


@dataclass
class Interim(Data):
    """Represents the Brazilian election results in interim state of processing.

    This object pre-processes the Brazilian election results.

    Attributes
    ----------
        candidacy_pos: str
            The candidacy position [presidente, governador]
        candidates: List[str]
            List of candidates ids
        aggregation_level: str
            The data geogrephical level of aggrevation
        geocoding_api: str
            The geocoding api to be used (Google Maps: GMAPS, OpenStreep Map: OSM)
    """

    candidacy_pos: str = None
    candidates: List[str] = field(default_factory=list)
    aggregation_level: str = None
    geocoding_api: str = None
    __results_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    __locations_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    __list_results_data: List[pd.DataFrame] = field(default_factory=list)

    def _read_results_csv(self, data_filename) -> pd.DataFrame:
        """Read the polling places.csv file and returns a pandas dataframe"""
        filepath = join(
            self._get_process_folder_path(state="raw"),
            self.data_name,
            data_filename,
        )
        self.__results_data = pd.read_csv(
            filepath,
            sep=";",
            encoding="latin1",
            na_values=["#NULO#", -1, -3],
            low_memory=False,
        ).infer_objects()

    def _read_locations_csv(self) -> pd.DataFrame:
        """Reads location csv from processed state folder"""
        filepath = join(
            self._get_process_folder_path(state="processed"),
            "locations",
            self.aggregation_level,
            f"locations_{self.geocoding_api}.csv",
        )
        self.__locations_data = pd.read_csv(filepath).infer_objects()

    def _rename_cols(self) -> pd.DataFrame:
        """Rename columns"""
        self.__results_data.rename(columns=MAP_COL_RENAME, inplace=True)
        self.__results_data = self.__results_data[MAP_COL_RENAME.values()]

    def _filter_by_candidacy_pos(self):
        """Filter election results by candidacy position"""
        self.__results_data = self.__results_data[
            self.__results_data["[ELECTION]_ID_CANDIDACY_POSITION"]
            == MAP_CANDIDACY[self.candidacy_pos]
        ]

    @staticmethod
    def _rename_votes_cols(votes) -> pd.DataFrame:
        map_rename_cols = {
            col: f"[ELECTION]_CANDIDATE_{int(col)}"
            for col in votes.columns
            if col not in MAP_BLANK_NULL.values() and col
        }
        map_rename_cols[MAP_BLANK_NULL["BLANK"]] = "[ELECTION]_BLANK"
        map_rename_cols[MAP_BLANK_NULL["NULL"]] = "[ELECTION]_NULL"
        return votes.rename(columns=map_rename_cols)

    def _get_votes_by_candidates(self) -> pd.DataFrame:
        """Get votes by candadidate"""
        votes = (
            self.__results_data.copy()
            .set_index(UNIQUE_ID + ["[ELECTION]_ID_CANDIDATE"])
            .unstack(fill_value=0)["[ELECTION]_VOTES"]
        )[self.candidates + list(MAP_BLANK_NULL.values())]

        return self._rename_votes_cols(votes)

    def _drop_duplicated_rows(self) -> pd.DataFrame:
        """Drop duplicated section rows from results data"""
        self.__results_data.drop_duplicates(
            subset=UNIQUE_ID,
            inplace=True,
        )

    def _join_votes(self, votes: pd.DataFrame) -> pd.DataFrame:
        """Join votes dataframe with results dataframe"""
        # Index data
        self.__results_data.set_index(
            keys=UNIQUE_ID,
            inplace=True,
        )
        # Join votes and data
        self.__results_data = self.__results_data.join(votes)
        self.__results_data.reset_index(inplace=True)
        self.__results_data.drop(
            [
                "[ELECTION]_VOTES",
                "[ELECTION]_ID_CANDIDACY_POSITION",
                "[ELECTION]_ID_CANDIDATE",
            ],
            axis=1,
            inplace=True,
        )

    def _create_candidates_shares(self):
        """Create candidate vote-shares columns"""
        candidates_cols = self._get_candidate_cols()
        for col in candidates_cols:
            self.__results_data[f"{col}_(%)"] = (
                100
                * self.__results_data[col]
                / self.__results_data["[ELECTION]_TURNOUT"]
            )

    def _create_blank_null_shares(self):
        """Create blank and null votes-shares"""
        self.__results_data["[ELECTION]_NULL_(%)"] = (
            100
            * self.__results_data["[ELECTION]_NULL"]
            / self.__results_data["[ELECTION]_TURNOUT"]
        )
        self.__results_data["[ELECTION]_BLANK_(%)"] = (
            100
            * self.__results_data["[ELECTION]_BLANK"]
            / self.__results_data["[ELECTION]_TURNOUT"]
        )

    def _create_turnout_abstention_shares(self):
        """Creates turnout and abstention shares"""
        self.__results_data["[ELECTION]_TURNOUT_(%)"] = (
            100
            * self.__results_data["[ELECTION]_TURNOUT"]
            / self.__results_data["[ELECTION]_ELECTORATE"]
        )
        self.__results_data["[ELECTION]_ABSTENTIONS_(%)"] = (
            100
            * self.__results_data["[ELECTION]_ABSTENTIONS"]
            / self.__results_data["[ELECTION]_ELECTORATE"]
        )

    def _create_shares_attributes(self):
        """Creates all share attributes"""
        self._create_candidates_shares()
        self._create_blank_null_shares()
        self._create_turnout_abstention_shares()

    def _get_candidate_cols(self):
        return [c for c in self.__results_data.columns if "CANDIDATE" in c]

    def _drop_na_cols(self) -> pd.DataFrame:
        """Drop all columns with NaN"""
        self.__results_data.dropna(axis=1, inplace=True)

    def _drop_na_candidates(self) -> pd.DataFrame:
        """Drop NaN candidates created as columns"""
        self.__results_data.dropna(
            subset=["[ELECTION]_ID_CANDIDATE"], axis=0, inplace=True
        )

    def _fill_na_electorate_biometry(self) -> pd.DataFrame:
        """Fill electorate biometry column in results data nans with zero"""
        self.__results_data["[ELECTION]_ELECTORATE_BIOMETRIA"].fillna(0, inplace=True)

    def _convert_cols_to_str(self) -> pd.DataFrame:
        """Convert all columns names from results data to str"""
        self.__results_data.columns = self.__results_data.columns.astype(str)

    def _pre_processing_data(self):
        """Pre Processing the elections results"""
        self.logger_info("Pre-processing elections results.")
        raw_dir = join(self._get_state_folders_path(state="raw"), self.data_name)
        filenames = self._get_files_in_id(raw_dir)
        for filename in tqdm(filenames, desc="Pre-Processing", leave=False):
            # Load raw data
            self._read_results_csv(filename)
            self._rename_cols()
            self._filter_by_candidacy_pos()
            self._drop_na_candidates()
            self._fill_na_electorate_biometry()
            votes = self._get_votes_by_candidates()
            self._drop_duplicated_rows()
            self._join_votes(votes)
            self._drop_na_cols()
            # Save the data as csv
            self.__list_results_data.append(self.__results_data.copy())

    def _concatenate_list_results_data(self) -> pd.DataFrame:
        """Concatenate the election results data in onw"""
        self.__results_data = pd.concat(self.__list_results_data)
        self._convert_cols_to_str()
        self.__list_results_data = []

    def _create_aggregation_map(self) -> Dict[str, str]:
        """Creates an aggregation map for the results data"""
        return {
            col: (
                "sum"
                if is_numeric_dtype(self.__results_data[col]) and "_ID_" not in col
                else "first"
            )
            for col in self.__results_data.columns
        }

    def _get_merging_keys(self) -> List[str]:
        """Generates the merging keys columns depending on the aggregatiopn level"""
        merging_keys = {
            "polling place": [
                "[GEO]_UF",
                "[GEO]_CITY",
                "[GEO]_ID_POLLING_ZONE",
                "[GEO]_ID_POLLING_PLACE",
            ],
            "city": ["[GEO]_UF", "[GEO]_CITY"],
        }
        return merging_keys[self.aggregation_level]

    def _aggregate_data(self) -> pd.DataFrame:
        """Aggregate the results data considering the aggregation level paramenter"""
        self.logger_info(f"Aggregating data by {self.aggregation_level}.")
        group_keys = self._get_merging_keys()
        agg_map = self._create_aggregation_map()
        self.__results_data = self.__results_data.groupby(by=group_keys).agg(agg_map)

    def _get_not_common_cols(self) -> List[str]:
        """Get the columns in the location data that does not exist in the results data"""
        return [
            col
            for col in self.__locations_data.columns
            if col not in self.__results_data.columns
        ]

    def _merge_results_and_location_data(self):
        """Merge results data with location data"""
        self.logger_info("Merging results and location data.")
        # Load data with geocode information from polling places
        self._read_locations_csv()
        merging_keys = self._get_merging_keys()
        self.__locations_data.set_index(merging_keys, inplace=True)
        not_commom_cols = self._get_not_common_cols()
        self.__results_data = self.__results_data.join(
            self.__locations_data[not_commom_cols]
        )

    def _remove_unecessary_cols(self):
        """Remove unecessary cols"""
        unecessary_cols = {
            "city": [col for col in self.__results_data if "POLLING" in col]
        }
        if unecessary_cols.get(self.aggregation_level):
            self.__results_data.drop(
                unecessary_cols.get(self.aggregation_level), axis=1, inplace=True
            )

    def _save_results_data(self):
        """Save results data"""
        self.__results_data.to_csv(
            join(self.cur_dir, f"data_{self.geocoding_api}.csv"), index=False
        )

    def _generate_pandas_profiling(self):
        """Generates pandas profiling"""
        self.logger_info("Generating profiling.")
        self.__results_data.reset_index(drop=True, inplace=True)
        profiling = ProfileReport(df=self.__results_data, minimal=True)
        profiling.to_file(join(self.cur_dir, "profiling.html"))

    def run(self):
        """Run interim process"""
        self.init_logger_name(msg="Results (Interim)")
        self.init_state(state="interim")
        self.logger_info("Generating interim data.")
        self._make_folders(
            folders=[self.data_name, self.aggregation_level, self.candidacy_pos.lower()]
        )
        self._pre_processing_data()
        self._concatenate_list_results_data()
        self._aggregate_data()
        self._create_shares_attributes()
        self._merge_results_and_location_data()
        self._remove_unecessary_cols()
        self._save_results_data()
        self._generate_pandas_profiling()
