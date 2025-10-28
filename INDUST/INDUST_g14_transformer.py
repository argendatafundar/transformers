from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['exportaciones_industriales'], axis=1),
	multiplicar_por_escalar(col='prop', k=100),
	query(condition="geocodigoFundar == 'ARG'"),
	replace_multiple_values(col='lall_desc_full', replacements={'Total manufacturas': 'Total', 'Manufacturas basadas en recursos naturales': 'Basadas en RRNN', 'Manufacturas de alta tecnología': 'Alta tecnología', 'Manufacturas de media tecnología': 'Media tecnología', 'Manufacturas de baja tecnología': 'Baja tecnología'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 62580 entries, 0 to 62579
#  Data columns (total 6 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   anio                        62580 non-null  int64  
#   1   geocodigoFundar             62580 non-null  object 
#   2   geonombreFundar             62580 non-null  object 
#   3   lall_desc_full              62580 non-null  object 
#   4   exportaciones_industriales  62090 non-null  float64
#   5   prop                        62085 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | lall_desc_full     |   exportaciones_industriales |    prop |
#  |---:|-------:|:------------------|:------------------|:-------------------|-----------------------------:|--------:|
#  |  0 |   1962 | AFG               | Afganistán        | Total manufacturas |                  1.44588e+07 | 0.17697 |
#  
#  ------------------------------
#  
#  drop_col(col=['exportaciones_industriales'], axis=1)
#  RangeIndex: 62580 entries, 0 to 62579
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             62580 non-null  int64  
#   1   geocodigoFundar  62580 non-null  object 
#   2   geonombreFundar  62580 non-null  object 
#   3   lall_desc_full   62580 non-null  object 
#   4   prop             62085 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | lall_desc_full     |   prop |
#  |---:|-------:|:------------------|:------------------|:-------------------|-------:|
#  |  0 |   1962 | AFG               | Afganistán        | Total manufacturas | 17.697 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop', k=100)
#  RangeIndex: 62580 entries, 0 to 62579
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             62580 non-null  int64  
#   1   geocodigoFundar  62580 non-null  object 
#   2   geonombreFundar  62580 non-null  object 
#   3   lall_desc_full   62580 non-null  object 
#   4   prop             62085 non-null  float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | lall_desc_full     |   prop |
#  |---:|-------:|:------------------|:------------------|:-------------------|-------:|
#  |  0 |   1962 | AFG               | Afganistán        | Total manufacturas | 17.697 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar == 'ARG'")
#  Index: 310 entries, 5 to 61711
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             310 non-null    int64  
#   1   geocodigoFundar  310 non-null    object 
#   2   geonombreFundar  310 non-null    object 
#   3   lall_desc_full   310 non-null    object 
#   4   prop             310 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | lall_desc_full     |    prop |
#  |---:|-------:|:------------------|:------------------|:-------------------|--------:|
#  |  5 |   1962 | ARG               | Argentina         | Total manufacturas | 17.9953 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='lall_desc_full', replacements={'Total manufacturas': 'Total', 'Manufacturas basadas en recursos naturales': 'Basadas en RRNN', 'Manufacturas de alta tecnología': 'Alta tecnología', 'Manufacturas de media tecnología': 'Media tecnología', 'Manufacturas de baja tecnología': 'Baja tecnología'})
#  Index: 310 entries, 5 to 61711
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             310 non-null    int64  
#   1   geocodigoFundar  310 non-null    object 
#   2   geonombreFundar  310 non-null    object 
#   3   lall_desc_full   310 non-null    object 
#   4   prop             310 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | lall_desc_full   |    prop |
#  |---:|-------:|:------------------|:------------------|:-----------------|--------:|
#  |  5 |   1962 | ARG               | Argentina         | Total            | 17.9953 |
#  
#  ------------------------------
#  