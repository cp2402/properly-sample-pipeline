import ssl
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile


def download_zip_file(
    url: str, file_name: str, dest_path: str, is_insecure=False
) -> None:
    """Download zip file from url and extract file to target destination"""

    if is_insecure:
        # ignore invalid certificates
        ssl._create_default_https_context = ssl._create_unverified_context

    with urlopen(url) as zipresp:
        with ZipFile(BytesIO(zipresp.read())) as zip_file:
            zip_file.extract(file_name, dest_path)
