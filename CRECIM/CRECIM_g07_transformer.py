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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition="iso3 == 'ARG'"),
	drop_col(col='iso3', axis=1),
	rename_cols(map={'participacion': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2942 entries, 0 to 2941
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           2942 non-null   int64  
#   1   iso3           2942 non-null   object 
#   2   participacion  2942 non-null   float64
#  
#  |    |   anio | iso3   |   participacion |
#  |---:|-------:|:-------|----------------:|
#  |  0 |   1950 | AFG    |      0.00111344 |
#  
#  ------------------------------
#  
#  query(condition="iso3 == 'ARG'")
#  Index: 21 entries, 62 to 82
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           21 non-null     int64  
#   1   iso3           21 non-null     object 
#   2   participacion  21 non-null     float64
#  
#  |    |   anio | iso3   |   participacion |
#  |---:|-------:|:-------|----------------:|
#  | 62 |   1820 | ARG    |     0.000722989 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 21 entries, 62 to 82
#  Data columns (total 2 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           21 non-null     int64  
#   1   participacion  21 non-null     float64
#  
#  |    |   anio |   participacion |
#  |---:|-------:|----------------:|
#  | 62 |   1820 |     0.000722989 |
#  
#  ------------------------------
#  
#  rename_cols(map={'participacion': 'valor'})
#  Index: 21 entries, 62 to 82
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    21 non-null     int64  
#   1   valor   21 non-null     float64
#  
#  |    |   anio |     valor |
#  |---:|-------:|----------:|
#  | 62 |   1820 | 0.0722989 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 21 entries, 62 to 82
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    21 non-null     int64  
#   1   valor   21 non-null     float64
#  
#  |    |   anio |     valor |
#  |---:|-------:|----------:|
#  | 62 |   1820 | 0.0722989 |
#  
#  ------------------------------
#  