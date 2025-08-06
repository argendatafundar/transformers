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
	query(condition='anio >= 1900'),
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
#  query(condition='anio >= 1900')
#  Index: 14886 entries, 0 to 21585
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  14886 non-null  object 
#   1   geonombreFundar  14886 non-null  object 
#   2   anio             14886 non-null  int64  
#   3   pib_per_capita   14886 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |
#  |---:|:------------------|:------------------|-------:|-----------------:|
#  |  0 | AFG               | Afganistán        |   1950 |             1156 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar'], axis=1)
#  Index: 14886 entries, 0 to 21585
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  14886 non-null  object 
#   1   anio             14886 non-null  int64  
#   2   pib_per_capita   14886 non-null  float64
#  
#  |    | geonombreFundar   |   anio |   pib_per_capita |
#  |---:|:------------------|-------:|-----------------:|
#  |  0 | Afganistán        |   1950 |             1156 |
#  
#  ------------------------------
#  