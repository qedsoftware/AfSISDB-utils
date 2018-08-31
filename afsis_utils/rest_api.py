import requests
from urllib.request import urlopen
from pathlib import Path
from tqdm import tqdm
from typing import Tuple

REQUEST_BASE_URL = 'https://afsisdb.qed.ai/cabinet/api/sample/' \
                   '?group={group}&machine={machine}'


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
                base_url: str=REQUEST_BASE_URL):
    out_path = Path(out_path)
    request_url = base_url.format(group=group, machine=machine)
    for ssn, url in tqdm(urls_generator(request_url, credentials)):
        data = fetch_from_bucket(url)
        save_file(out_path, ssn, data)
