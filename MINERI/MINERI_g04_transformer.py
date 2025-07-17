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
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def ordenar_categorica(df, col1:str, order1:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    return df.sort_values(by=[col1])

@transformer.convert
def replace_na(df:DataFrame, col:str, replace_with:int):
    df = df.fillna(value={col : replace_with})
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def complete_rows(df: DataFrame, col1:str, col2:str):
    import itertools as it
    new_df = pd.DataFrame(list(it.product(df[col1].unique(), df[col2].unique())), columns=[col1, col2])
    df = pd.merge(new_df, df, how='left',on=[col1, col2])
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	complete_rows(col1='anio', col2='grupo_nuevo'),
	rename_cols(map={'grupo_nuevo': 'indicador', 'expo_grupo': 'valor'}),
	multiplicar_por_escalar(col='valor', k=1e-06),
	replace_value(col='indicador', curr_value='cobre', new_value='Cobre'),
	replace_value(col='indicador', curr_value='oro', new_value='Oro'),
	replace_value(col='indicador', curr_value='plata', new_value='Plata'),
	replace_value(col='indicador', curr_value='otro', new_value='Otro'),
	replace_value(col='indicador', curr_value='litio', new_value='Litio'),
	replace_na(col='valor', replace_with=0),
	ordenar_categorica(col1='indicador', order1=['Oro', 'Plata', 'Litio', 'Cobre', 'Otro']),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 139 entries, 0 to 138
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   grupo_nuevo  139 non-null    object 
#   1   anio         139 non-null    int64  
#   2   expo_grupo   139 non-null    float64
#  
#  |    | grupo_nuevo   |   anio |   expo_grupo |
#  |---:|:--------------|-------:|-------------:|
#  |  0 | cobre         |   1994 |  3.63523e+06 |
#  
#  ------------------------------
#  
#  complete_rows(col1='anio', col2='grupo_nuevo')
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         145 non-null    int64  
#   1   grupo_nuevo  145 non-null    object 
#   2   expo_grupo   139 non-null    float64
#  
#  |    |   anio | grupo_nuevo   |   expo_grupo |
#  |---:|-------:|:--------------|-------------:|
#  |  0 |   1994 | cobre         |  3.63523e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'grupo_nuevo': 'indicador', 'expo_grupo': 'valor'})
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   indicador  145 non-null    object 
#   2   valor      139 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | cobre       | 3.63523 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   indicador  145 non-null    object 
#   2   valor      139 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | cobre       | 3.63523 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='cobre', new_value='Cobre')
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   indicador  145 non-null    object 
#   2   valor      139 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | Cobre       | 3.63523 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='oro', new_value='Oro')
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   indicador  145 non-null    object 
#   2   valor      139 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | Cobre       | 3.63523 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='plata', new_value='Plata')
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   indicador  145 non-null    object 
#   2   valor      139 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | Cobre       | 3.63523 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='otro', new_value='Otro')
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   indicador  145 non-null    object 
#   2   valor      139 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | Cobre       | 3.63523 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='litio', new_value='Litio')
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   indicador  145 non-null    object 
#   2   valor      139 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | Cobre       | 3.63523 |
#  
#  ------------------------------
#  
#  replace_na(col='valor', replace_with=0)
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       145 non-null    int64   
#   1   indicador  145 non-null    category
#   2   valor      145 non-null    float64 
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | Cobre       | 3.63523 |
#  
#  ------------------------------
#  
#  ordenar_categorica(col1='indicador', order1=['Oro', 'Plata', 'Litio', 'Cobre', 'Otro'])
#  Index: 145 entries, 72 to 33
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       145 non-null    int64   
#   1   indicador  145 non-null    category
#   2   valor      145 non-null    float64 
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  | 72 |   2008 | Oro         | 702.913 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   anio       145 non-null    int64   
#   1   indicador  145 non-null    category
#   2   valor      145 non-null    float64 
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | Oro         | 7.09784 |
#  
#  ------------------------------
#  