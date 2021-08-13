"""Generate raw data for election results"""
from os.path import join
from typing import List
import re
import zipfile
from dataclasses import dataclass, field
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from tqdm import tqdm
from src.election import Election


@dataclass
class Raw(Election):
    """Represents the Brazilian polling places in raw processing state.

    This object downloads the Brazilian election results.

    Attributes
    ----------
        url: The url to collect the raw data
        html: the html page where the raw data can be downloaded
        links: list of links to download raw data
    """

    url_data: str = None
    __html: str = None
    __links: List[str] = field(default_factory=list)

    def _fill_url(self) -> str:
        """Fill the gaps in the election data url link"""
        return self.url_data.format(self.year, self.round)

    def _download_html(self) -> None:
        """Donwload the election results page"""
        self.url_data = self._fill_url()
        self.__html = urlopen(self.url_data).read().decode("utf-8")

    def _get_links(self) -> None:
        """Get the links from the html"""
        soup = BeautifulSoup(self.__html, "html.parser")
        for link in soup.findAll("a", attrs={"href": re.compile(r"\.zip$")}):
            self.__links.append(link.get("href"))

    def _download_raw_data(self) -> None:
        """Donwload raw election data"""
        self.logger_info("Downloading raw data.")
        for link in tqdm(self.__links, desc="Downloading", leave=False):
            name_file = link.split("/")[-1]
            urlretrieve(link, join(self.cur_dir, name_file))

    def _unzip_raw_data(self) -> None:
        """Unzip only the csv raw data in the current directory"""
        self.logger_info("Unzipping raw data.")
        list_filename = self._get_files_in_cur_dir()
        for zip_filename in tqdm(list_filename, desc="Unziping", leave=False):
            with zipfile.ZipFile(join(self.cur_dir, zip_filename), "r") as zip_ref:
                for filename in zip_ref.namelist():
                    if filename.endswith(".csv"):
                        zip_ref.extract(filename, self.cur_dir)

    def _remove_zip_files(self) -> None:
        """Remove all zip files in the current directory"""
        self.logger_info("Removing zip files.")
        list_filename = self._get_files_in_cur_dir()
        for filename in list_filename:
            if filename.endswith(".zip"):
                self._remove_file_from_cur_dir(filename=filename)

    def _rename_raw_data(self) -> None:
        """Rename all files in the current directory"""
        self.logger_info("Renameing csv files .")
        list_filename = self._get_files_in_cur_dir()
        for old_filename in list_filename:
            new_filename = old_filename.split("_")[2] + ".csv"
            self._rename_file_from_cur_dir(
                old_filename=old_filename, new_filename=new_filename
            )

    def _empty_folder_run(self):
        """Run without files in the working directory"""
        self._download_html()
        self._get_links()
        self._download_raw_data()
        self._unzip_raw_data()
        self._remove_zip_files()
        self._rename_raw_data()

    def run(self) -> None:
        """Generate election raw data"""
        self.init_logger_name(msg="Results (Raw)")
        self.init_state(state="raw")
        self.logger_info("Generating raw data.")
        self._make_folders(folders=[self.data_name])
        files_exist = self._get_files_in_cur_dir()
        if not files_exist:
            self._empty_folder_run()
        else:
            self.logger_warning(
                "Non empty directory, the process only runs on empty folders!"
            )
