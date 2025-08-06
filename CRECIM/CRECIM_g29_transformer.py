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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="geocodigoFundar.isin( ['USA','GBR','ARG','AUS','WLD','WEU_MPD'])"),
	drop_col(col=['geocodigoFundar'], axis=1),
	rename_cols(map={'geonombreFundar': 'categoria', 'pib_per_capita': 'valor'})
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
#  query(condition="geocodigoFundar.isin( ['USA','GBR','ARG','AUS','WLD','WEU_MPD'])")
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1403 non-null   object 
#   1   geonombreFundar  1403 non-null   object 
#   2   anio             1403 non-null   int64  
#   3   pib_per_capita   1403 non-null   float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |
#  |----:|:------------------|:------------------|-------:|-----------------:|
#  | 296 | ARG               | Argentina         |   1800 |             1484 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar'], axis=1)
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  1403 non-null   object 
#   1   anio             1403 non-null   int64  
#   2   pib_per_capita   1403 non-null   float64
#  
#  |     | geonombreFundar   |   anio |   pib_per_capita |
#  |----:|:------------------|-------:|-----------------:|
#  | 296 | Argentina         |   1800 |             1484 |
#  
#  ------------------------------
#  
#  rename_cols(map={'geonombreFundar': 'categoria', 'pib_per_capita': 'valor'})
#  Index: 1403 entries, 296 to 21585
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  1403 non-null   object 
#   1   anio       1403 non-null   int64  
#   2   valor      1403 non-null   float64
#  
#  |     | categoria   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 296 | Argentina   |   1800 |    1484 |
#  
#  ------------------------------
#  