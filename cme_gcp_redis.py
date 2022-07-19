import pandas as pd
import numpy as np
from highland.data import redis_reader_threaded as reader
import threading

LOCK = threading.Lock()

class Adapter():
    def __init__(self, rds, limit=None):
        self.rds = rds
        self.df = None
        reader.start(debug=False)

    def __len__(self):
        return 0

    def get_dataframe(self, start, stop):
        return self.df

    def chunk_generator(self):
        raise NotImplementedError

    def get_stream(self):
        return self.stream()

    def stream(self):
        while reader.wait():
            cache, _ = reader.get()
            cache_as_np = np.asarray([ [float(x[1][b'p']), float(x[1][b'v']), int(x[1][b'seq'])] for x in cache], dtype=float)

            df = pd.DataFrame(data=cache_as_np, columns=[ "mdEntryPx", "mdEntrySize", "seq" ])
            df['transactionTime'] = [x[1][b't'].decode("utf-8") for x in cache]
            df['transactionTime'] = pd.to_datetime(df['transactionTime'])
            df['session'] = 1 - (
                (df.transactionTime.dt.dayofweek < 6) & 
                (df.transactionTime.dt.time > pd.to_datetime('13:30:00.000000').time()) & 
                (df.transactionTime.dt.time < pd.to_datetime('20:00:00.000000').time())
            ).astype(int)
            self.df = df

            yield df
