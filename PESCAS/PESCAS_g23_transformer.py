from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def imput_na(df: DataFrame, col:str, value:str) -> DataFrame:

    from numpy import nan

    df.loc[df[col] == value,col] = nan
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	imput_na(col='share_acuicola', value=0.0),
	drop_na(col='share_acuicola')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 13486 entries, 0 to 13485
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            13486 non-null  int64  
#   1   iso3            13486 non-null  object 
#   2   pais_nombre     13486 non-null  object 
#   3   share_acuicola  13486 non-null  float64
#  
#  |    |   anio | iso3   | pais_nombre            |   share_acuicola |
#  |---:|-------:|:-------|:-----------------------|-----------------:|
#  |  0 |   1961 | ARE    | Emiratos Árabes Unidos |                0 |
#  
#  ------------------------------
#  
#  imput_na(col='share_acuicola', value=0.0)
#  RangeIndex: 13486 entries, 0 to 13485
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            13486 non-null  int64  
#   1   iso3            13486 non-null  object 
#   2   pais_nombre     13486 non-null  object 
#   3   share_acuicola  8738 non-null   float64
#  
#  |    |   anio | iso3   | pais_nombre            |   share_acuicola |
#  |---:|-------:|:-------|:-----------------------|-----------------:|
#  |  0 |   1961 | ARE    | Emiratos Árabes Unidos |              nan |
#  
#  ------------------------------
#  
#  drop_na(col='share_acuicola')
#  Index: 8738 entries, 2 to 13485
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            8738 non-null   int64  
#   1   iso3            8738 non-null   object 
#   2   pais_nombre     8738 non-null   object 
#   3   share_acuicola  8738 non-null   float64
#  
#  |    |   anio | iso3   | pais_nombre   |   share_acuicola |
#  |---:|-------:|:-------|:--------------|-----------------:|
#  |  2 |   1961 | AUS    | Australia     |          10.2017 |
#  
#  ------------------------------
#  