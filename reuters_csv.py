import pandas as pd

class Adapter():
    def __init__(self, rds, limit=None):
        self.rds = rds
        vars=rds['adapter']
        self.path = vars.get('path')
        self.num_chunks = vars.get('num-chunks')
        self.resolution = vars.get('resolution')
        self.full_df = pd.read_csv(self.path, index_col=0, parse_dates=["Date-Time"])
        self.full_df.columns = [ "transactionTime", "mdEntryPx", "mdEntrySize" ]
        self.full_df = self.full_df.dropna()
        self.full_df.index = range(len(self.full_df))
        self.shape = self.full_df.shape
        self.total_sample_count = len(self.full_df)

    def __len__(self):
        return self.total_sample_count

    def get_dataframe(self, start, stop):
        df = self.full_df[start:stop]
        df['session'] = 1 - (
            (df.transactionTime.dt.dayofweek < 6) & 
            (df.transactionTime.dt.time > pd.to_datetime('13:30:00.000000').time()) & 
            (df.transactionTime.dt.time < pd.to_datetime('20:00:00.000000').time())
        ).astype(int)
        return df

    def chunk_generator(self):
        chunk_count = self.num_chunks
        chunk_size = self.total_sample_count // chunk_count
        lst = range(0, self.total_sample_count)
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size][0], lst[i:i + chunk_size][-1]
        yield None, None
