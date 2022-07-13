import h5py
import pandas as pd

TICK_PATH = "/data/legacy/v8/converted_hdf5_files/ticks_to_6-14-2021.hdf5"
ACC_PATH = "/data/legacy/v8/converted_hdf5_files/p_acc.hdf5"

class Adapter():
    def __init__(self, rds, limit=None):
        self.tick_file = h5py.File(TICK_PATH, 'r')
        self.p_acc = h5py.File(ACC_PATH, 'r')
        self.shape = self.tick_file['pv'].shape
        self.rds = rds
        vars=rds['adapter']
        self.num_chunks = vars.get('num-chunks')
        self.resolution = vars.get('resolution')
        self.total_sample_count = self.shape[0]

    def __del__(self):
        self.tick_file.close()
        self.p_acc.close()

    def __len__(self):
        return self.total_sample_count

    def get_dataframe(self, start, stop):
        df = pd.DataFrame(data=self.tick_file['pv'][start:stop], columns=[ "mdEntryPx", "mdEntrySize" ])
        df.index = range(start, stop)
        df['transactionTime'] = self.tick_file['time'][start:stop, 0].astype('datetime64[ns]') 
        df['session'] = 1 - (
            (df.transactionTime.dt.dayofweek < 6) & 
            (df.transactionTime.dt.time > pd.to_datetime('13:30:00.000000').time()) & 
            (df.transactionTime.dt.time < pd.to_datetime('20:00:00.000000').time())
        ).astype(int)
        return df

    def chunk_generator(self):
        chunk_size = self.total_sample_count // 200 # 200 is arbitrary replace
        lst = range(0, self.total_sample_count)
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]
