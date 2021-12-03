from util.data_util import DataSet

if __name__ == "__main__":
    ds = DataSet('cleaned_data.zip')
    ds.load_data(2)