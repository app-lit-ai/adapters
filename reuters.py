import pandas as pd

class DataSource():
    def __init__(self, rds, limit=None):
        self.rds = rds
        vars=rds['adapter']
        self.path = vars.get('path')
        self.full_df = pd.read_csv(self.path, index_col=0)
        self.shape = self.full_df.shape
        self.total_sample_count = len(self.full_df)

    def __len__(self):
        return self.total_sample_count

    def get_dataframe(self, start, stop):
        return self.full_df[start:stop]

    def chunk_generator(self):
        chunk_size = self.total_sample_count // 200 # 200 is arbitrary replace
        lst = range(0, self.total_sample_count)
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    def split_by_year(input_path, output_folder, start_year=None):
        chunks = pd.read_csv(input_path, chunksize=1e6, usecols=["Date-Time", "Price", "Volume"], parse_dates=["Date-Time"])
        current_year = start_year
        matching_dfs = []
        for df in chunks:
            start_timestamp, end_timestamp = df['Date-Time'].iloc[0], df['Date-Time'].iloc[len(df)-1]
            start_year, end_year = start_timestamp.year, end_timestamp.year
            print(f"{start_timestamp} -> {end_timestamp}")
            if end_year < current_year:
                continue
            if start_year != current_year:
                if current_year and len(matching_dfs) > 0:
                    pd.concat(matching_dfs).to_csv(f"{output_folder}/{current_year}.csv")
                current_year = start_year
            match = df[df['Date-Time'].dt.year == current_year]
            if len(match) > 0:
                matching_dfs.append(match)
            if start_year != end_year:
                pd.concat(matching_dfs).to_csv(f"{output_folder}/{current_year}.csv")
                current_year = end_year
                match = df[df['Date-Time'].dt.year == current_year]
                if len(match) > 0:
                    matching_dfs.append(match)        