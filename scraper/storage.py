import pandas as pd
import os

class Storage:
    def save_to_csv(self, data, filename="results.csv"):
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        print(f"Saved {len(data)} records to {filename}")
