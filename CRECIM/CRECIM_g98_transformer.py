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
	query(condition="geocodigoFundar.isin(['ALB','ARG','AUS','AUT','BEL','BOL','BRA','CAN','CHE','CHL','CHN','COL','CSK','DEU','DNK','ECU','ESP','FIN','FRA','GBR','GHA','GRC','HUN','IDN','IND','ISL','ITA','JAM','JPN','LBR','LKA','MEX','MYS','NGA','NLD','NOR','NZL','PER','POL','PRT','ROU','RUS','SGP','SVU','SWE','URY','USA','VEN','SER','ZAF','EAS_MPD','EEU_MPD','LAC_MPD','SEA_MPD','WEU_MPD','WOF_MPD','WLD'])"),
	query(condition='anio in [1900, 1925, 1950, 1975, 2000, 2022]'),
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
#  query(condition="geocodigoFundar.isin(['ALB','ARG','AUS','AUT','BEL','BOL','BRA','CAN','CHE','CHL','CHN','COL','CSK','DEU','DNK','ECU','ESP','FIN','FRA','GBR','GHA','GRC','HUN','IDN','IND','ISL','ITA','JAM','JPN','LBR','LKA','MEX','MYS','NGA','NLD','NOR','NZL','PER','POL','PRT','ROU','RUS','SGP','SVU','SWE','URY','USA','VEN','SER','ZAF','EAS_MPD','EEU_MPD','LAC_MPD','SEA_MPD','WEU_MPD','WOF_MPD','WLD'])")
#  Index: 12607 entries, 146 to 21585
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  12607 non-null  object 
#   1   geonombreFundar  12607 non-null  object 
#   2   anio             12607 non-null  int64  
#   3   pib_per_capita   12607 non-null  float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |
#  |----:|:------------------|:------------------|-------:|-----------------:|
#  | 146 | ALB               | Albania           |   1870 |              711 |
#  
#  ------------------------------
#  
#  query(condition='anio in [1900, 1925, 1950, 1975, 2000, 2022]')
#  Index: 325 entries, 148 to 21585
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  325 non-null    object 
#   1   geonombreFundar  325 non-null    object 
#   2   anio             325 non-null    int64  
#   3   pib_per_capita   325 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |
#  |----:|:------------------|:------------------|-------:|-----------------:|
#  | 148 | ALB               | Albania           |   1900 |             1092 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar'], axis=1)
#  Index: 325 entries, 148 to 21585
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  325 non-null    object 
#   1   anio             325 non-null    int64  
#   2   pib_per_capita   325 non-null    float64
#  
#  |     | geonombreFundar   |   anio |   pib_per_capita |
#  |----:|:------------------|-------:|-----------------:|
#  | 148 | Albania           |   1900 |             1092 |
#  
#  ------------------------------
#  