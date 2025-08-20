from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="variable in ('gini','loggnip')"),
	rename_cols(map={'variable': 'indicador'}),
	drop_col(col='pais', axis=1),
	drop_col(col='geocodigoFundar', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 648 entries, 0 to 647
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  648 non-null    object 
#   1   geonombreFundar  648 non-null    object 
#   2   pais             648 non-null    object 
#   3   variable         648 non-null    object 
#   4   valor            456 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar    | pais             | variable   |   valor |
#  |---:|:------------------|:-------------------|:-----------------|:-----------|--------:|
#  |  0 | PNG               | Papúa Nueva Guinea | Papua New Guinea | gini       |   41.85 |
#  
#  ------------------------------
#  
#  query(condition="variable in ('gini','loggnip')")
#  Index: 324 entries, 0 to 645
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  324 non-null    object 
#   1   geonombreFundar  324 non-null    object 
#   2   pais             324 non-null    object 
#   3   variable         324 non-null    object 
#   4   valor            304 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar    | pais             | variable   |   valor |
#  |---:|:------------------|:-------------------|:-----------------|:-----------|--------:|
#  |  0 | PNG               | Papúa Nueva Guinea | Papua New Guinea | gini       |   41.85 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'indicador'})
#  Index: 324 entries, 0 to 645
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  324 non-null    object 
#   1   geonombreFundar  324 non-null    object 
#   2   pais             324 non-null    object 
#   3   indicador        324 non-null    object 
#   4   valor            304 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar    | pais             | indicador   |   valor |
#  |---:|:------------------|:-------------------|:-----------------|:------------|--------:|
#  |  0 | PNG               | Papúa Nueva Guinea | Papua New Guinea | gini        |   41.85 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  Index: 324 entries, 0 to 645
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  324 non-null    object 
#   1   geonombreFundar  324 non-null    object 
#   2   indicador        324 non-null    object 
#   3   valor            304 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar    | indicador   |   valor |
#  |---:|:------------------|:-------------------|:------------|--------:|
#  |  0 | PNG               | Papúa Nueva Guinea | gini        |   41.85 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 324 entries, 0 to 645
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  324 non-null    object 
#   1   indicador        324 non-null    object 
#   2   valor            304 non-null    float64
#  
#  |    | geonombreFundar    | indicador   |   valor |
#  |---:|:-------------------|:------------|--------:|
#  |  0 | Papúa Nueva Guinea | gini        |   41.85 |
#  
#  ------------------------------
#  