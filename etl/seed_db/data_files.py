import pandas as pd

class DataFileTransformer:
    dataframe: pd.DataFrame

    def do_transformations(self):  
        pass

    def transform(self):
        self.do_transformations()
        self.dataframe.to_dict(orient='records')





    