from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    import numpy as np
    df = df.replace({col: values})
    return df

@transformer.convert
def ordenar_categorica(df, col1:str, order1:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    return df.sort_values(by=[col1])

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'grupo_nuevo': 'indicador', 'impo_grupo': 'valor'}),
	multiplicar_por_escalar(col='valor', k=1e-06),
	replace_values(col='indicador', values={'aluminio': 'Aluminio', 'cinc': 'Zinc', 'ferroaleaciones': 'Ferroaleaciones', 'hierro': 'Hierro', 'otros': 'Otro'}),
	ordenar_categorica(col1='indicador', order1=['Otro', 'Hierro', 'Aluminio', 'Ferroaleaciones', 'Zinc']),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   grupo_nuevo  145 non-null    object 
#   1   anio         145 non-null    int64  
#   2   impo_grupo   145 non-null    float64
#  
#  |    | grupo_nuevo   |   anio |   impo_grupo |
#  |---:|:--------------|-------:|-------------:|
#  |  0 | aluminio      |   1994 |  8.39544e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'grupo_nuevo': 'indicador', 'impo_grupo': 'valor'})
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  145 non-null    object 
#   1   anio       145 non-null    int64  
#   2   valor      145 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | aluminio    |   1994 | 8.39544 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  145 non-null    object 
#   1   anio       145 non-null    int64  
#   2   valor      145 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | aluminio    |   1994 | 8.39544 |
#  
#  ------------------------------
#  
#  replace_values(col='indicador', values={'aluminio': 'Aluminio', 'cinc': 'Zinc', 'ferroaleaciones': 'Ferroaleaciones', 'hierro': 'Hierro', 'otros': 'Otro'})
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   indicador  145 non-null    category
#   1   anio       145 non-null    int64   
#   2   valor      145 non-null    float64 
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Aluminio    |   1994 | 8.39544 |
#  
#  ------------------------------
#  
#  ordenar_categorica(col1='indicador', order1=['Otro', 'Hierro', 'Aluminio', 'Ferroaleaciones', 'Zinc'])
#  Index: 145 entries, 144 to 42
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   indicador  145 non-null    category
#   1   anio       145 non-null    int64   
#   2   valor      145 non-null    float64 
#  
#  |     | indicador   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 144 | Otro        |   2022 | 550.968 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   indicador  145 non-null    category
#   1   anio       145 non-null    int64   
#   2   valor      145 non-null    float64 
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Otro        |   1994 | 212.459 |
#  
#  ------------------------------
#  