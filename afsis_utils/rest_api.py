import requests
from urllib.request import urlopen
from pathlib import Path
from tqdm import tqdm
from typing import Tuple
from pandas import DataFrame

SPECTRA_URL = 'https://afsisdb.qed.ai/cabinet/api/sample/' \
              '?group={group}&machine={machine}'

WET_CHEM_URL = 'https://afsisdb.qed.ai/cabinet/api/wetchemistry/' \
               '?group=AfSIS'


def urls_generator(request_url, credentials):
    r = requests.get(request_url, auth=credentials).json()
    aws_urls = [(x['ssn'], x['binary_file']) for x in r['results']]
    for url in aws_urls:
        yield url
    while ('next' in r.keys()) and r['next']:
        r = requests.get(r['next'], auth=credentials).json()
        aws_urls = [(x['ssn'], x['binary_file']) for x in r['results']]
        for url in aws_urls:
            yield url


def wet_chem_generator(request_url, credentials):
    r = requests.get(request_url, auth=credentials).json()
    for url in r['results']:
        yield url
    while ('next' in r.keys()) and r['next']:
        r = requests.get(r['next'], auth=credentials).json()
        for url in r['results']:
            yield url


def save_file(opus_path: Path, ssn: str, data):
    out_path = opus_path / ssn
    with open(out_path, "wb") as file:
        file.write(data)


def fetch_from_bucket(opus_url: str):
    response = urlopen(opus_url)
    return response.read()


def get_spectra(group: str,
                machine: str,
                out_path: str,
                credentials: Tuple[str, str],
                base_url: str=SPECTRA_URL):
    out_path = Path(out_path)
    request_url = base_url.format(group=group, machine=machine)
    for ssn, url in tqdm(urls_generator(request_url, credentials)):
        data = fetch_from_bucket(url)
        save_file(out_path, ssn, data)


def get_wet_chemistry(group: str,
                      credentials: Tuple[str, str],
                      base_url: str=WET_CHEM_URL):
    request_url = base_url.format(group=group)
    samples = []
    for wet_chem in tqdm(wet_chem_generator(request_url, credentials)):
        samples.append(wet_chem)
    return DataFrame(samples)
