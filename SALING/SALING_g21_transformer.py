from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="variable.isin(['hombres1','mujeres1'])"),
	replace_value(col='variable', curr_value='hombres1', new_value='Varones', mapping=None),
	replace_value(col='variable', curr_value='mujeres1', new_value='Mujeres', mapping=None),
	ordenar_dos_columnas(col1='edad', order1=[25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75], col2='variable', order2=['Varones', 'Mujeres'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 204 entries, 0 to 203
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   edad      204 non-null    int64  
#   1   variable  204 non-null    object 
#   2   valor     204 non-null    float64
#  
#  |    |   edad | variable   |   valor |
#  |---:|-------:|:-----------|--------:|
#  |  0 |     25 | hombres1   |  360178 |
#  
#  ------------------------------
#  
#  query(condition="variable.isin(['hombres1','mujeres1'])")
#  Index: 102 entries, 0 to 201
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   edad      102 non-null    int64  
#   1   variable  102 non-null    object 
#   2   valor     102 non-null    float64
#  
#  |    |   edad | variable   |   valor |
#  |---:|-------:|:-----------|--------:|
#  |  0 |     25 | hombres1   |  360178 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='hombres1', new_value='Varones', mapping=None)
#  Index: 102 entries, 0 to 201
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   edad      102 non-null    int64  
#   1   variable  102 non-null    object 
#   2   valor     102 non-null    float64
#  
#  |    |   edad | variable   |   valor |
#  |---:|-------:|:-----------|--------:|
#  |  0 |     25 | Varones    |  360178 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='mujeres1', new_value='Mujeres', mapping=None)
#  Index: 102 entries, 0 to 201
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype   
#  ---  ------    --------------  -----   
#   0   edad      102 non-null    category
#   1   variable  102 non-null    category
#   2   valor     102 non-null    float64 
#  
#  |    |   edad | variable   |   valor |
#  |---:|-------:|:-----------|--------:|
#  |  0 |     25 | Varones    |  360178 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='edad', order1=[25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75], col2='variable', order2=['Varones', 'Mujeres'])
#  Index: 102 entries, 0 to 201
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype   
#  ---  ------    --------------  -----   
#   0   edad      102 non-null    category
#   1   variable  102 non-null    category
#   2   valor     102 non-null    float64 
#  
#  |    |   edad | variable   |   valor |
#  |---:|-------:|:-----------|--------:|
#  |  0 |     25 | Varones    |  360178 |
#  
#  ------------------------------
#  