from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'code': 'geocodigo', 'indicador': 'valor'}),
	drop_col(col=['orden', 'pais'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 109 entries, 0 to 108
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  109 non-null    object 
#   1   geonombreFundar  109 non-null    object 
#   2   orden            109 non-null    int64  
#   3   pais             109 non-null    object 
#   4   indicador        109 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   orden | pais   |   indicador |
#  |---:|:------------------|:------------------|--------:|:-------|------------:|
#  |  0 | BTN               | Bhután            |     109 | Bhutan |        0.21 |
#  
#  ------------------------------
#  
#  rename_cols(map={'code': 'geocodigo', 'indicador': 'valor'})
#  RangeIndex: 109 entries, 0 to 108
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  109 non-null    object 
#   1   geonombreFundar  109 non-null    object 
#   2   orden            109 non-null    int64  
#   3   pais             109 non-null    object 
#   4   valor            109 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   orden | pais   |   valor |
#  |---:|:------------------|:------------------|--------:|:-------|--------:|
#  |  0 | BTN               | Bhután            |     109 | Bhutan |    0.21 |
#  
#  ------------------------------
#  
#  drop_col(col=['orden', 'pais'], axis=1)
#  RangeIndex: 109 entries, 0 to 108
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  109 non-null    object 
#   1   geonombreFundar  109 non-null    object 
#   2   valor            109 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   valor |
#  |---:|:------------------|:------------------|--------:|
#  |  0 | BTN               | Bhután            |    0.21 |
#  
#  ------------------------------
#  