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
	drop_col(col='location_name_short_en', axis=1),
	rename_cols(map={'x_tt_pc': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12912 entries, 0 to 12911
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    12912 non-null  int64  
#   1   iso3                    12912 non-null  object 
#   2   location_name_short_en  12912 non-null  object 
#   3   x_tt_pc                 12912 non-null  float64
#  
#  |    |   anio | iso3   | location_name_short_en   |   x_tt_pc |
#  |---:|-------:|:-------|:-------------------------|----------:|
#  |  0 |   1962 | AFG    | Afghanistan              | 0.0649777 |
#  
#  ------------------------------
#  
#  query(condition="iso3 == 'ARG'")
#  Index: 60 entries, 5 to 12685
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    60 non-null     int64  
#   1   iso3                    60 non-null     object 
#   2   location_name_short_en  60 non-null     object 
#   3   x_tt_pc                 60 non-null     float64
#  
#  |    |   anio | iso3   | location_name_short_en   |   x_tt_pc |
#  |---:|-------:|:-------|:-------------------------|----------:|
#  |  5 |   1962 | ARG    | Argentina                |   1.03658 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 60 entries, 5 to 12685
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    60 non-null     int64  
#   1   location_name_short_en  60 non-null     object 
#   2   x_tt_pc                 60 non-null     float64
#  
#  |    |   anio | location_name_short_en   |   x_tt_pc |
#  |---:|-------:|:-------------------------|----------:|
#  |  5 |   1962 | Argentina                |   1.03658 |
#  
#  ------------------------------
#  
#  drop_col(col='location_name_short_en', axis=1)
#  Index: 60 entries, 5 to 12685
#  Data columns (total 2 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   anio     60 non-null     int64  
#   1   x_tt_pc  60 non-null     float64
#  
#  |    |   anio |   x_tt_pc |
#  |---:|-------:|----------:|
#  |  5 |   1962 |   1.03658 |
#  
#  ------------------------------
#  
#  rename_cols(map={'x_tt_pc': 'valor'})
#  Index: 60 entries, 5 to 12685
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    60 non-null     int64  
#   1   valor   60 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  5 |   1962 | 1.03658 |
#  
#  ------------------------------
#  