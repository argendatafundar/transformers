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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio in [1950, 1975, 2000, 2022]'),
	query(condition='geonombreFundar in ["Argentina", "Mundo", "Estados Unidos", "Europa Occidental","Reino Unido", "Australia"]'),
	drop_col(col=['geocodigoFundar'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21586 entries, 0 to 21585
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  21586 non-null  object 
#   1   geonombreFundar  21586 non-null  object 
#   2   anio             21586 non-null  int64  
#   3   pib_per_capita   21586 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |
#  |---:|:------------------|:------------------|-------:|-----------------:|
#  |  0 | AFG               | Afganistán        |   1950 |             1156 |
#  
#  ------------------------------
#  
#  query(condition='anio in [1950, 1975, 2000, 2022]')
#  Index: 664 entries, 0 to 21585
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  664 non-null    object 
#   1   geonombreFundar  664 non-null    object 
#   2   anio             664 non-null    int64  
#   3   pib_per_capita   664 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |
#  |---:|:------------------|:------------------|-------:|-----------------:|
#  |  0 | AFG               | Afganistán        |   1950 |             1156 |
#  
#  ------------------------------
#  
#  query(condition='geonombreFundar in ["Argentina", "Mundo", "Estados Unidos", "Europa Occidental","Reino Unido", "Australia"]')
#  Index: 22 entries, 376 to 21585
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  22 non-null     object 
#   1   geonombreFundar  22 non-null     object 
#   2   anio             22 non-null     int64  
#   3   pib_per_capita   22 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |
#  |----:|:------------------|:------------------|-------:|-----------------:|
#  | 376 | ARG               | Argentina         |   1950 |             7949 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar'], axis=1)
#  Index: 22 entries, 376 to 21585
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  22 non-null     object 
#   1   anio             22 non-null     int64  
#   2   pib_per_capita   22 non-null     float64
#  
#  |     | geonombreFundar   |   anio |   pib_per_capita |
#  |----:|:------------------|-------:|-----------------:|
#  | 376 | Argentina         |   1950 |             7949 |
#  
#  ------------------------------
#  