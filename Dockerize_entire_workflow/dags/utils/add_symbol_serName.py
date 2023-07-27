import pandas as pd
from save_parquet import save_parquet

#read metal symbol files
metal_symbol = pd.read_csv('./data/symbols_valid_meta.csv')
metal_symbol = metal_symbol[['Symbol', 'Security Name']]
#correct some wrong spelling, coresponding to Stock file name
metal_symbol['Symbol'] = metal_symbol['Symbol'].str.replace('$', '-',regex=False)
metal_symbol['Symbol'] = metal_symbol['Symbol'].str.replace('.V', '',regex=False)
#creat mapping dictionary
symbol_mapping = metal_symbol.set_index('Symbol').to_dict()['Security Name']
#retain features columns
features = ['Symbol', 'Security Name', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
#path to save processed dataset
path = './data/processed_stocks_etfs/'

def add_name(file):
    name = file.stem
    df = pd.read_csv(file)
    df['Symbol'] = name
    df['Security Name'] = df['Symbol'].map(symbol_mapping)
    #df.name = name
    #return df
    save_parquet(df[features], name, path)