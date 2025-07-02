from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='gerd_gdp', k=100),
	drop_col(col=['geocodigoFundar'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 159 entries, 0 to 158
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         159 non-null    object 
#   1   geonombreFundar         159 non-null    object 
#   2   ultimo_anio_disponible  159 non-null    int64  
#   3   gerd_gdp                159 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ultimo_anio_disponible |   gerd_gdp |
#  |---:|:------------------|:------------------|-------------------------:|-----------:|
#  |  0 | ISR               | Israel            |                     2022 |  0.0601924 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='gerd_gdp', k=100)
#  RangeIndex: 159 entries, 0 to 158
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         159 non-null    object 
#   1   geonombreFundar         159 non-null    object 
#   2   ultimo_anio_disponible  159 non-null    int64  
#   3   gerd_gdp                159 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ultimo_anio_disponible |   gerd_gdp |
#  |---:|:------------------|:------------------|-------------------------:|-----------:|
#  |  0 | ISR               | Israel            |                     2022 |    6.01924 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar'], axis=1)
#  RangeIndex: 159 entries, 0 to 158
#  Data columns (total 3 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geonombreFundar         159 non-null    object 
#   1   ultimo_anio_disponible  159 non-null    int64  
#   2   gerd_gdp                159 non-null    float64
#  
#  |    | geonombreFundar   |   ultimo_anio_disponible |   gerd_gdp |
#  |---:|:------------------|-------------------------:|-----------:|
#  |  0 | Israel            |                     2022 |    6.01924 |
#  
#  ------------------------------
#  