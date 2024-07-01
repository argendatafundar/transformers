from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_na(df: DataFrame, col:str):
    df = df.dropna(subset= col, axis=0)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo'}),
	drop_col(col='countryname', axis=1),
	rename_cols(map={'tradeofgdp': 'valor'}),
	drop_na(col='valor'),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         16960 non-null  int64  
#   1   iso3         16960 non-null  object 
#   2   countryname  16960 non-null  object 
#   3   tradeofgdp   10698 non-null  float64
#  
#  |    |   anio | iso3   | countryname   |   tradeofgdp |
#  |---:|-------:|:-------|:--------------|-------------:|
#  |  0 |   2023 | ABW    | Aruba         |          nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         16960 non-null  int64  
#   1   geocodigo    16960 non-null  object 
#   2   countryname  16960 non-null  object 
#   3   tradeofgdp   10698 non-null  float64
#  
#  |    |   anio | geocodigo   | countryname   |   tradeofgdp |
#  |---:|-------:|:------------|:--------------|-------------:|
#  |  0 |   2023 | ABW         | Aruba         |          nan |
#  
#  ------------------------------
#  
#  drop_col(col='countryname', axis=1)
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        16960 non-null  int64  
#   1   geocodigo   16960 non-null  object 
#   2   tradeofgdp  10698 non-null  float64
#  
#  |    |   anio | geocodigo   |   tradeofgdp |
#  |---:|-------:|:------------|-------------:|
#  |  0 |   2023 | ABW         |          nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'tradeofgdp': 'valor'})
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       16960 non-null  int64  
#   1   geocodigo  16960 non-null  object 
#   2   valor      10698 non-null  float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2023 | ABW         |     nan |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 10698 entries, 1 to 16959
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       10698 non-null  int64  
#   1   geocodigo  10698 non-null  object 
#   2   valor      10698 non-null  float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  1 |   2022 | ABW         | 160.459 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 10698 entries, 0 to 10697
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       10698 non-null  int64  
#   1   geocodigo  10698 non-null  object 
#   2   valor      10698 non-null  float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1960 | GHA         | 63.6364 |
#  
#  ------------------------------
#  