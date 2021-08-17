"""Generate raw data for census"""
import shutil
import os
from typing import List
import re
import zipfile
from dataclasses import dataclass, field
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from tqdm import tqdm
from src.data import Data


@dataclass
class Raw(Data):
    """Represents the Brazilian census data in raw processing state.

    This object downloads the Brazilian census data.

    Attributes
    ----------
        url: The url to collect the raw data
        html: the html page where the raw data can be downloaded
        links: list of links to download raw data
    """

    url_data: str = None
    __html: str = None
    __links: List[str] = field(default_factory=list)

    def _download_html(self) -> None:
        """Donwload the census page"""
        self.__html = urlopen(self.url_data).read().decode("utf-8")

    def _get_links(self) -> None:
        """Get the links from the html"""
        soup = BeautifulSoup(self.__html, "html.parser")
        for link in soup.findAll("a", attrs={"href": re.compile(r"\.zip$")}):
            self.__links.append(link.get("href"))

    def _download_raw_data(self) -> None:
        """Donwload raw census data"""
        self.logger_info("Downloading raw data.")
        if self.__links:
            for link in tqdm(self.__links, desc="Downloading", leave=False):
                urlretrieve(
                    os.path.join(self.url_data, link), os.path.join(self.cur_dir, link)
                )
        else:
            self.logger_error("No download links, check the url_data parameter.")
            exit()

    def _unzip_raw_data(self) -> None:
        """Unzip only the csv raw data in the current directory"""
        self.logger_info("Unzipping raw data.")
        list_filename = self._get_files_in_cur_dir()
        for zip_filename in tqdm(list_filename, desc="Unziping", leave=False):
            with zipfile.ZipFile(
                os.path.join(self.cur_dir, zip_filename), "r"
            ) as zip_ref:
                self._mkdir(zip_filename.split(".")[0])
                for member in zip_ref.namelist():
                    filename = os.path.basename(member)
                    if not filename:
                        continue

                    if filename.endswith(".csv"):
                        source = zip_ref.open(member)
                        target = open(os.path.join(self.cur_dir, filename), "wb")
                        with source, target:
                            shutil.copyfileobj(source, target)

            self.cur_dir = os.path.join(
                self._get_state_folders_path("raw"), self.data_name
            )

    def _remove_zip_files(self) -> None:
        """Remove all zip files in the current directory"""
        self.logger_info("Removing zip files.")
        list_filename = self._get_files_in_cur_dir()
        for filename in list_filename:
            if filename.endswith(".zip"):
                self._remove_file_from_cur_dir(filename=filename)

    def _remove_empty_folders(self) -> None:
        """Remove empty folders from cur_dir"""
        folders = self._get_folders_in_cur_dir()
        for folder in folders:
            folder_path = os.path.join(self.cur_dir, folder)
            if not os.listdir(folder_path):
                shutil.rmtree(folder_path)

    def _rename_raw_data(self) -> None:
        """Rename all files in the current directory"""
        self.logger_info("Renameing csv files.")
        for folder in self._get_folders_in_cur_dir():
            self.cur_dir = os.path.join(self.cur_dir, folder)
            list_filename = self._get_files_in_cur_dir()
            for old_filename in list_filename:
                new_filename = old_filename.split("_")[0] + ".csv"
                self._rename_file_from_cur_dir(
                    old_filename=old_filename, new_filename=new_filename.upper()
                )
            self.cur_dir = os.path.join(
                self._get_state_folders_path("raw"), self.data_name
            )

    def _empty_folder_run(self):
        """Run without files in the working directory"""
        self._download_html()
        self._get_links()
        self._download_raw_data()
        self._unzip_raw_data()
        self._remove_zip_files()
        self._remove_empty_folders()
        self._rename_raw_data()

    def run(self) -> None:
        """Generate census raw data"""
        self.init_logger_name(msg="Census (Raw)")
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
