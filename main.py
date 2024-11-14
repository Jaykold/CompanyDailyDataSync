import pandas as pd
from config import Config

def load_dataset(data_path):
    df = pd.DataFrame(data_path)

# if __name__=="__main__":
#     utils = Config()
#     utils.data_path