from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
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
drop_col(col='countryname', axis=1),
	query(condition='iso3 == "ARG"'),
	drop_col(col='iso3', axis=1),
	rename_cols(map={'servicesexportsbop_pc_v2': 'valor'}),
	drop_na(col='valor'),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 4 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   anio                      16960 non-null  int64  
#   1   iso3                      16960 non-null  object 
#   2   countryname               16960 non-null  object 
#   3   servicesexportsbop_pc_v2  9077 non-null   float64
#  
#  |    |   anio | iso3   | countryname   |   servicesexportsbop_pc_v2 |
#  |---:|-------:|:-------|:--------------|---------------------------:|
#  |  0 |   2023 | ABW    | Aruba         |                        nan |
#  
#  ------------------------------
#  
#  drop_col(col='countryname', axis=1)
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   anio                      16960 non-null  int64  
#   1   iso3                      16960 non-null  object 
#   2   servicesexportsbop_pc_v2  9077 non-null   float64
#  
#  |    |   anio | iso3   |   servicesexportsbop_pc_v2 |
#  |---:|-------:|:-------|---------------------------:|
#  |  0 |   2023 | ABW    |                        nan |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 64 entries, 576 to 639
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   anio                      64 non-null     int64  
#   1   iso3                      64 non-null     object 
#   2   servicesexportsbop_pc_v2  47 non-null     float64
#  
#  |     |   anio | iso3   |   servicesexportsbop_pc_v2 |
#  |----:|-------:|:-------|---------------------------:|
#  | 576 |   2023 | ARG    |                        nan |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 64 entries, 576 to 639
#  Data columns (total 2 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   anio                      64 non-null     int64  
#   1   servicesexportsbop_pc_v2  47 non-null     float64
#  
#  |     |   anio |   servicesexportsbop_pc_v2 |
#  |----:|-------:|---------------------------:|
#  | 576 |   2023 |                        nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'servicesexportsbop_pc_v2': 'valor'})
#  Index: 64 entries, 576 to 639
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    64 non-null     int64  
#   1   valor   47 non-null     float64
#  
#  |     |   anio |   valor |
#  |----:|-------:|--------:|
#  | 576 |   2023 |     nan |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 47 entries, 577 to 639
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    47 non-null     int64  
#   1   valor   47 non-null     float64
#  
#  |     |   anio |   valor |
#  |----:|-------:|--------:|
#  | 577 |   2022 | 14.0644 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 47 entries, 0 to 46
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    47 non-null     int64  
#   1   valor   47 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1976 | 15.4145 |
#  
#  ------------------------------
#  