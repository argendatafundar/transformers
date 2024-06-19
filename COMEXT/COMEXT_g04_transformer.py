from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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
query(condition="iso3 == 'ARG'"),
	drop_col(col='iso3', axis=1),
	drop_col(col='countryname', axis=1),
	rename_cols(map={'exportsofgoodsandservicesofgdp': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 4 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   anio                            16960 non-null  int64  
#   1   iso3                            16960 non-null  object 
#   2   countryname                     16960 non-null  object 
#   3   exportsofgoodsandservicesofgdp  10756 non-null  float64
#  
#  |    |   anio | iso3   | countryname                 |   exportsofgoodsandservicesofgdp |
#  |---:|-------:|:-------|:----------------------------|---------------------------------:|
#  |  0 |   2023 | AFE    | Africa Eastern and Southern |                              nan |
#  
#  ------------------------------
#  
#  query(condition="iso3 == 'ARG'")
#  Index: 64 entries, 3520 to 3583
#  Data columns (total 4 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   anio                            64 non-null     int64  
#   1   iso3                            64 non-null     object 
#   2   countryname                     64 non-null     object 
#   3   exportsofgoodsandservicesofgdp  63 non-null     float64
#  
#  |      |   anio | iso3   | countryname   |   exportsofgoodsandservicesofgdp |
#  |-----:|-------:|:-------|:--------------|---------------------------------:|
#  | 3520 |   2023 | ARG    | Argentina     |                              nan |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 64 entries, 3520 to 3583
#  Data columns (total 3 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   anio                            64 non-null     int64  
#   1   countryname                     64 non-null     object 
#   2   exportsofgoodsandservicesofgdp  63 non-null     float64
#  
#  |      |   anio | countryname   |   exportsofgoodsandservicesofgdp |
#  |-----:|-------:|:--------------|---------------------------------:|
#  | 3520 |   2023 | Argentina     |                              nan |
#  
#  ------------------------------
#  
#  drop_col(col='countryname', axis=1)
#  Index: 64 entries, 3520 to 3583
#  Data columns (total 2 columns):
#   #   Column                          Non-Null Count  Dtype  
#  ---  ------                          --------------  -----  
#   0   anio                            64 non-null     int64  
#   1   exportsofgoodsandservicesofgdp  63 non-null     float64
#  
#  |      |   anio |   exportsofgoodsandservicesofgdp |
#  |-----:|-------:|---------------------------------:|
#  | 3520 |   2023 |                              nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'exportsofgoodsandservicesofgdp': 'valor'})
#  Index: 64 entries, 3520 to 3583
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    64 non-null     int64  
#   1   valor   63 non-null     float64
#  
#  |      |   anio |   valor |
#  |-----:|-------:|--------:|
#  | 3520 |   2023 |     nan |
#  
#  ------------------------------
#  