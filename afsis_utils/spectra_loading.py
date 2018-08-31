import numpy as np

from os import listdir
from os.path import basename, splitext
from brukeropusreader import opus_reader
from tqdm import tqdm
from pathlib import Path
from pandas import DataFrame


def change_ssn(raw_ssn: str, prefix: str) -> str:
    return prefix + splitext(basename(raw_ssn))[0]


def parse_many_spectra(spectra_dir: str,
                       prefix: str='',
                       wave_info=(501, 3996, 1715)
                       ) -> DataFrame:
    all_spectra = {}
    unparsed_spectra = 0
    spectra_names = listdir(spectra_dir)
    spectra_dir = Path(spectra_dir)

    for spectrum_name in tqdm(spectra_names):
        try:
            spectrum_path = spectra_dir / spectrum_name
            spectrum = opus_reader(spectrum_path)
            absorbance = spectrum.interpolate(*wave_info)[1]
            all_spectra[change_ssn(spectrum_name, prefix)] = absorbance
        except Exception:
            unparsed_spectra += 1

    print(f'Parsing finished. '
          f'{unparsed_spectra} spectra not parsed')
    columns = np.linspace(*wave_info) \
        .astype(str)
    spectra_df = DataFrame.from_dict(all_spectra,
                                     orient='index')
    spectra_df.columns = columns
    return spectra_df
