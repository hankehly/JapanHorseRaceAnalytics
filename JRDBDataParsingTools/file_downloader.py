import requests
from pydantic import BaseModel, HttpUrl, constr, validator, ConfigDict
import pathlib
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import zipfile
import io
import datetime
from JRDBDataParsingTools.structured_logger import logger


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

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    @validator("download_dir")
    def validate_download_dir(cls, v):
        path = pathlib.Path(v)
        if not path.is_absolute():
            raise ValueError("download_dir must be an absolute path")
        return v


def download_and_extract_files(webpage_url, username, password, download_dir) -> None:
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

    logger.debug("Processing year files")
    for link in links:
        file_link = link.get("href")
        if is_year_file(file_link):
            download_and_extract(
                str(args.webpage_url),
                file_link,
                args.username,
                args.password,
                str(args.download_dir),
            )
            covered_years.add(extract_year(file_link))

    logger.debug("Processing date files")
    for link in links:
        file_link = link.get("href")
        if is_date_file(link.get("href")):
            date = extract_date(file_link)
            if date.year not in covered_years:
                download_and_extract(
                    str(args.webpage_url),
                    file_link,
                    args.username,
                    args.password,
                    str(args.download_dir),
                )
