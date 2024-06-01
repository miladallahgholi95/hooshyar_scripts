import zip_unicode
from pathlib import Path
import logging
import getpass
import sys
import os

# Disable chardet logger
logging.getLogger('chardet').level = logging.ERROR

# Config our logger
logging.basicConfig(format='%(message)s', stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('zip_unicode')


class ZipExtractor(zip_unicode.ZipHandler):

    def extract_all(self, destination: Path = None):
        """Extract content of zipfile with readable filename"""
        password = self.password
        destination = destination or self.destination

        if self.is_encrypted() and not password:
            password = getpass.getpass().encode()

        for original_name, decoded_name in self.name_map.items():
            if decoded_name.endswith("/"):
                # skip subdirectory
                continue

            logger.info(f"Extracting: {decoded_name}")
            fo = destination / decoded_name
            fo.parent.mkdir(parents=True, exist_ok=True)
            extract_ok = self._extract_individual(original_name, fo, password)
            if not extract_ok:
                continue
        else:
            logger.info("Finished")


def extractor(zip_file_path, destination_path):
    try:
        print('extracting')
        zip_ref = ZipExtractor(path=zip_file_path, extract_path=destination_path)
        zip_ref.extract_all()
    except Exception as e:
        print('error: ', e)
