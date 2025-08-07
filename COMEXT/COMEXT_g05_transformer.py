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
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition="geocodigoFundar == 'ARG'"),
	drop_col(col='geocodigoFundar', axis=1),
	drop_col(col='location_name_short_en', axis=1),
	rename_cols(map={'x_tt_pc': 'valor'})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 12912 entries, 0 to 12911
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         12912 non-null  object 
#   1   geonombreFundar         12912 non-null  object 
#   2   anio                    12912 non-null  int64  
#   3   location_name_short_en  12912 non-null  object 
#   4   x_tt_pc                 12912 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | location_name_short_en   |   x_tt_pc |
#  |---:|:------------------|:------------------|-------:|:-------------------------|----------:|
#  |  0 | AFG               | Afganistán        |   1962 | Afghanistan              | 0.0649777 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar == 'ARG'")
#  Index: 60 entries, 5 to 12685
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         60 non-null     object 
#   1   geonombreFundar         60 non-null     object 
#   2   anio                    60 non-null     int64  
#   3   location_name_short_en  60 non-null     object 
#   4   x_tt_pc                 60 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | location_name_short_en   |   x_tt_pc |
#  |---:|:------------------|:------------------|-------:|:-------------------------|----------:|
#  |  5 | ARG               | Argentina         |   1962 | Argentina                |   1.03658 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 60 entries, 5 to 12685
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geonombreFundar         60 non-null     object 
#   1   anio                    60 non-null     int64  
#   2   location_name_short_en  60 non-null     object 
#   3   x_tt_pc                 60 non-null     float64
#  
#  |    | geonombreFundar   |   anio | location_name_short_en   |   x_tt_pc |
#  |---:|:------------------|-------:|:-------------------------|----------:|
#  |  5 | Argentina         |   1962 | Argentina                |   1.03658 |
#  
#  ------------------------------
#  
#  drop_col(col='location_name_short_en', axis=1)
#  Index: 60 entries, 5 to 12685
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  60 non-null     object 
#   1   anio             60 non-null     int64  
#   2   x_tt_pc          60 non-null     float64
#  
#  |    | geonombreFundar   |   anio |   x_tt_pc |
#  |---:|:------------------|-------:|----------:|
#  |  5 | Argentina         |   1962 |   1.03658 |
#  
#  ------------------------------
#  
#  rename_cols(map={'x_tt_pc': 'valor'})
#  Index: 60 entries, 5 to 12685
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  60 non-null     object 
#   1   anio             60 non-null     int64  
#   2   valor            60 non-null     float64
#  
#  |    | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|-------:|--------:|
#  |  5 | Argentina         |   1962 | 1.03658 |
#  
#  ------------------------------
#  