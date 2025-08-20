from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def long_to_wide(df:DataFrame, index:list[str], columns:str, values:str):
    df = df.pivot(index=index, columns=columns, values=values).reset_index()
    df.index.name = None
    df.columns.name = None
    df.columns = [str(col) for col in df.columns]  # Convertir columnas a str
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
	query(condition="variable in ('gini', 'loggnip')"),
	drop_col(col='pais', axis=1),
	drop_col(col='geocodigoFundar', axis=1),
	long_to_wide(index=['geonombreFundar'], columns='variable', values='valor')
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
#  query(condition="variable in ('gini', 'loggnip')")
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
#  drop_col(col='pais', axis=1)
#  Index: 324 entries, 0 to 645
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  324 non-null    object 
#   1   geonombreFundar  324 non-null    object 
#   2   variable         324 non-null    object 
#   3   valor            304 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar    | variable   |   valor |
#  |---:|:------------------|:-------------------|:-----------|--------:|
#  |  0 | PNG               | Papúa Nueva Guinea | gini       |   41.85 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 324 entries, 0 to 645
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  324 non-null    object 
#   1   variable         324 non-null    object 
#   2   valor            304 non-null    float64
#  
#  |    | geonombreFundar    | variable   |   valor |
#  |---:|:-------------------|:-----------|--------:|
#  |  0 | Papúa Nueva Guinea | gini       |   41.85 |
#  
#  ------------------------------
#  
#  long_to_wide(index=['geonombreFundar'], columns='variable', values='valor')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  162 non-null    object 
#   1   gini             153 non-null    float64
#   2   loggnip          151 non-null    float64
#  
#  |    | geonombreFundar   |   gini |   loggnip |
#  |---:|:------------------|-------:|----------:|
#  |  0 | Albania           |  28.96 |      9.25 |
#  
#  ------------------------------
#  