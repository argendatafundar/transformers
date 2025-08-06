from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio in [1900, 1925, 1950, 1975, 2000, 2022]'),
	drop_col(col=['geocodigoFundar'], axis=1),
	sort_values(how='ascending', by=['anio', 'geonombreFundar'])
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
#  query(condition='anio in [1900, 1925, 1950, 1975, 2000, 2022]')
#  Index: 785 entries, 0 to 21585
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  785 non-null    object 
#   1   geonombreFundar  785 non-null    object 
#   2   anio             785 non-null    int64  
#   3   pib_per_capita   785 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |
#  |---:|:------------------|:------------------|-------:|-----------------:|
#  |  0 | AFG               | Afganistán        |   1950 |             1156 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar'], axis=1)
#  Index: 785 entries, 0 to 21585
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  785 non-null    object 
#   1   anio             785 non-null    int64  
#   2   pib_per_capita   785 non-null    float64
#  
#  |    | geonombreFundar   |   anio |   pib_per_capita |
#  |---:|:------------------|-------:|-----------------:|
#  |  0 | Afganistán        |   1950 |             1156 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geonombreFundar'])
#  RangeIndex: 785 entries, 0 to 784
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  785 non-null    object 
#   1   anio             785 non-null    int64  
#   2   pib_per_capita   785 non-null    float64
#  
#  |    | geonombreFundar   |   anio |   pib_per_capita |
#  |---:|:------------------|-------:|-----------------:|
#  |  0 | Albania           |   1900 |             1092 |
#  
#  ------------------------------
#  