import datetime
import io
import pathlib
import re
import zipfile
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, ConfigDict, HttpUrl, constr, validator

from jhra.utilities.structured_logger import logger


def is_year_file(filename) -> bool:
    return re.search(r"_(\d{4})\.zip", filename) is not None


def extract_year(filename) -> int:
    match = re.search(r"_(\d{4})\.zip", filename)
    return int(match.group(1))


def is_date_file(filename) -> bool:
    return re.search(r"(\d{6})\.zip", filename) is not None


def extract_date(filename) -> datetime.date:
    match = re.search(r"(\d{6})\.zip", filename)
    # How do we handle 2-digit years?
    # Function strptime() can parse 2-digit years when given %y format code.
    # When 2-digit years are parsed, they are converted according to the POSIX
    # and ISO C standards: values 69–99 are mapped to 1969–1999, and values 0–68
    # are mapped to 2000–2068.
    # https://docs.python.org/3/library/time.html
    return datetime.datetime.strptime(match.group(1), "%y%m%d").date()


def download_and_extract(base_url, file_link, username, password, download_dir):
    full_url = urljoin(base_url, file_link)
    logger.info(f"Downloading {full_url}")
    file_response = requests.get(full_url, auth=(username, password), stream=True)
    logger.debug(f"Response status code: {file_response.status_code}")
    if file_response.status_code == 200:
        with io.BytesIO(file_response.content) as file_bytes:
            logger.debug(f"Extracting files from {file_link}")
            with zipfile.ZipFile(file_bytes) as zip_ref:
                zip_ref.extractall(download_dir)


class DownloadAndExtractFilesArgs(BaseModel):
    webpage_url: HttpUrl
    username: constr(strip_whitespace=True, min_length=1)
    password: constr(strip_whitespace=True, min_length=1)
    download_dir: str
    start_date: datetime.datetime = None
    end_date: datetime.datetime = None

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    @validator("download_dir")
    def validate_download_dir(cls, v):
        path = pathlib.Path(v)
        if not path.is_absolute():
            raise ValueError("download_dir must be an absolute path")
        return v


def download_and_extract_files(
    webpage_url: str,
    username: str,
    password: str,
    download_dir: str,
    skip_year_files: bool = False,
    threads: int = None,
    start_date: datetime.date = None,
    end_date: datetime.date = None,
) -> None:
    """
    Download and extract all files from the JRDB website.

    Parameters
    ----------
    webpage_url : str
        The URL of the JRDB website.
    username : str
        The username to use for authentication.
    password : str
        The password to use for authentication.
    download_dir : str
        The directory to download and extract the files to.
    skip_year_files : bool, optional
        If True, skip downloading and extracting year files.
        Defaults to False.
    threads : int, optional
        The number of threads to use for downloading and extracting files.
        If None, use the number of CPUs on the system.
        Defaults to None.
    start_date : datetime.date, optional
        The start date to filter files. Only files with date greater than or equal to this will be processed.
        Defaults to None.
    end_date : datetime.date, optional
        The end date to filter files. Only files with date less than or equal to this will be processed.
        Defaults to None.

    Returns
    -------
    None

    """
    args = DownloadAndExtractFilesArgs(
        webpage_url=webpage_url,
        username=username,
        password=password,
        download_dir=download_dir,
    )

    logger.info(f"Downloading and extracting files from {args.webpage_url}")
    response = requests.get(args.webpage_url, auth=(args.username, args.password))

    logger.debug(f"Response status code: {response.status_code}")
    response.raise_for_status()

    logger.debug("Parsing webpage")
    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a")

    covered_years = set()

    if skip_year_files:
        logger.debug("Skipping year files")
    else:
        logger.debug("Processing year files")
        with ThreadPoolExecutor(max_workers=threads) as executor:
            for link in links:
                file_link = link.get("href")
                if is_year_file(file_link):
                    year = extract_year(file_link)
                    if start_date and year < start_date.year:
                        continue
                    if end_date and year > end_date.year:
                        continue
                    executor.submit(
                        download_and_extract,
                        str(args.webpage_url),
                        file_link,
                        args.username,
                        args.password,
                        str(args.download_dir),
                    )
                    covered_years.add(year)

    logger.debug("Processing date files")
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for link in links:
            file_link = link.get("href")
            if is_date_file(link.get("href")):
                date = extract_date(file_link)
                if start_date and date < start_date:
                    continue
                if end_date and date > end_date:
                    continue
                if date.year not in covered_years:
                    executor.submit(
                        download_and_extract,
                        str(args.webpage_url),
                        file_link,
                        args.username,
                        args.password,
                        str(args.download_dir),
                    )
