from datetime import timedelta
from pandas._libs.tslibs.timestamps import Timestamp
import pandas as pd
import h5py

class DataSource():
    def __init__(self, rds, limit=None):
        self.rds = rds
        vars=rds['datasource']
        self.path = vars.get('path')
        self.num_chunks = vars.get('num-chunks')
        self.resolution = vars.get('resolution')
        self.window = vars.get('window')
        self.total_sample_count = 0
        self.df = None
        self.offset = 0
        with h5py.File(self.path, "r") as h5:
            samples_in_file = h5['data']['table'].shape[0]
            # guaranteed to only have one source file per instantiation by design
            if limit:
                self.df = pd.read_hdf(self.path, start=0, stop=limit)
            else:
                self.df = pd.read_hdf(self.path)
            self.df.columns = ['symbol', 'session', 'securityID', 'transactionTime', 'msgSeqNum', 'rptSeqNum','mdEntryPx', 'mdEntrySize', 'sendingTime']
            self.df = self.df.assign(transactionTime=lambda x: pd.to_datetime(x.transactionTime))
            self.df = self.df.assign(sendingTime=lambda x: pd.to_datetime(x.sendingTime))
            self.df = self.df.assign(date=lambda x: pd.to_datetime(x.transactionTime).view('int64'))
            self.df['is_cash'] = self.df.session == 0 # 0 = cash trading session (9:30am-4:00pm EST)
            self.df['is_globex'] = self.df.session == 1 # 1 = globex trading session (all hours outside of that)
            # self.df = self.df[['mdEntryPx', 'mdEntrySize', 'transactionTime', 'date', 'is_cash', 'is_globex']]
            self.total_sample_count += samples_in_file

        self.TIME_CACHE = { }
        self.OTHER_TIME_CACHE = { }

    def __len__(self):
        return self.total_sample_count

    def get_dataframe(self, start, stop):        
        return self.df[start:stop]

    def get_range(self, date):
        if date.day in self.TIME_CACHE:
            start, stop = self.TIME_CACHE[date.day]['start'], self.TIME_CACHE[date.day]['stop']
        else:
            start = date.replace(hour=14, minute=30, second=0, microsecond=0).value
            stop = date.replace(hour=21, minute=00, second=0, microsecond=0).value
            self.TIME_CACHE[date.day] = {
                'start': start,
                'stop': stop
            }
        return start, stop

    def get_market_times(self, date:Timestamp):
        if date.day in self.OTHER_TIME_CACHE:
            prev_close, next_open = self.OTHER_TIME_CACHE[date.day]['prev_close'], self.OTHER_TIME_CACHE[date.day]['next_open']
        else:
            open_time = date.replace(hour=8, minute=30, second=0, microsecond=0) # 15ms
            next_open = (open_time + timedelta(days=1)).value
            close_time = open_time.replace(hour=15, minute=00, second=0, microsecond=0) # 15ms
            prev_close = (close_time - timedelta(days=1)).value
            self.OTHER_TIME_CACHE[date.day] = {
                'prev_close': prev_close,
                'next_open': next_open
            }
        return prev_close, next_open

    def chunk_generator(self):
        chunk_count = self.num_chunks
        chunk_size = self.total_sample_count // chunk_count
        lst = range(0, self.total_sample_count)
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size][0], lst[i:i + chunk_size][-1]
        yield None, None