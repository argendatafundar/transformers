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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='countryname', axis=1),
	query(condition='iso3 == "ARG"'),
	drop_col(col='iso3', axis=1),
	rename_cols(map={'servicesexportsbop_pc_v2': 'valor'})
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
#  |    |   anio | iso3   | countryname                 |   servicesexportsbop_pc_v2 |
#  |---:|-------:|:-------|:----------------------------|---------------------------:|
#  |  0 |   2023 | AFE    | Africa Eastern and Southern |                        nan |
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
#  |  0 |   2023 | AFE    |                        nan |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 64 entries, 3520 to 3583
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   anio                      64 non-null     int64  
#   1   iso3                      64 non-null     object 
#   2   servicesexportsbop_pc_v2  47 non-null     float64
#  
#  |      |   anio | iso3   |   servicesexportsbop_pc_v2 |
#  |-----:|-------:|:-------|---------------------------:|
#  | 3520 |   2023 | ARG    |                        nan |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 64 entries, 3520 to 3583
#  Data columns (total 2 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   anio                      64 non-null     int64  
#   1   servicesexportsbop_pc_v2  47 non-null     float64
#  
#  |      |   anio |   servicesexportsbop_pc_v2 |
#  |-----:|-------:|---------------------------:|
#  | 3520 |   2023 |                        nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'servicesexportsbop_pc_v2': 'valor'})
#  Index: 64 entries, 3520 to 3583
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    64 non-null     int64  
#   1   valor   47 non-null     float64
#  
#  |      |   anio |   valor |
#  |-----:|-------:|--------:|
#  | 3520 |   2023 |     nan |
#  
#  ------------------------------
#  