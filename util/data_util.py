'''
Author: Adam Shedivy
Stat 451 group project 2021

'''


import os
from typing import Optional, List
from numpy.lib.shape_base import _make_along_axis_idx
import pandas as pd
from pathlib import Path
import zipfile, csv


class DataSet(object):

    REPO_ROOT = 'CrimeAnalysisML'
    DATA_TAG  = data_dir = 'data/{}/data_{}.csv'

    def __init__(self, dataset: str) -> None:
        self.dataset = dataset
        self.data_path = os.path.join(os.getcwd(), 'data', self.dataset)
        self._assert_path()

    def _assert_path(self):
        if not os.path.isfile(self.data_path):
            raise FileNotFoundError(f"{self.data_path} does not exist, make sure it is the correct file \
                and that it is within the data directory")


    def load_data(self, num_years: int = 21) -> pd.DataFrame:
        """load data into dataframe

        Args:
            num_years (int, optional): number of years to download. Defaults to Optional[int].

        Returns:
            pd.DataFrame: new dataframe
        """
        print(self.data_path)

        m_data = []
        first_read = True
        header = 0
        idx = 0
        with zipfile.ZipFile(self.data_path, 'r') as zf:
            files = [file for file in zf.namelist() if file.endswith('.csv')][1:]
            for filename in files:
                if idx == num_years:
                    break

                print(f"downloading: {filename} ...")
                with zf.open(filename, 'r') as f:
                    if first_read:
                        first_read = False
                    else:
                        header = 1
                        
                    for line in f.readlines()[header:]:
                        row = csv.StringIO(line.decode().rstrip())
                        reader = csv.reader(row, delimiter=',')
                        split_str = list(reader)[0]
                        m_data.append(split_str)
                idx += 1
        print('done.')
        zf.close()

        print('build DataFrame ... ')
        df = pd.DataFrame(m_data[1:], columns=m_data[0])
        return df

    
    def load_subset(self, subset: List[str]) -> pd.DataFrame:
        """load subset of data given list of years

        Args:
            subset (List[str]): list of years

        Returns:
            pd.DataFrame: new dataframe
        """
        print(self.data_path)
        m_data = []
        first_read = True
        header = 0
        idx = 0
        with zipfile.ZipFile(self.data_path, 'r') as zf:
            files = [self.DATA_TAG.format(year, year) for year in subset]
            for filename in files:

                print(f"downloading: {filename} ...")
                with zf.open(filename, 'r') as f:
                    if first_read:
                        first_read = False
                    else:
                        header = 1
                        
                    for line in f.readlines()[header:]:
                        row = csv.StringIO(line.decode().rstrip())
                        reader = csv.reader(row, delimiter=',')
                        split_str = list(reader)[0]
                        m_data.append(split_str)
                idx += 1
        print('done.')
        zf.close()
        
        print('build DataFrame ...')
        df = pd.DataFrame(m_data[1:], columns=m_data[0])
        return df
