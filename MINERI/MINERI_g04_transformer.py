from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def complete_rows(df: DataFrame, col1:str, col2:str):
    import itertools as it
    import pandas as pd
    new_df = pd.DataFrame(list(it.product(df[col1].unique(), df[col2].unique())), columns=[col1, col2])
    df = pd.merge(new_df, df, how='left',on=[col1, col2])
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_na(df:DataFrame, col:str, replace_with:int):
    df = df.fillna(value={col : replace_with})
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def str_to_title(df: DataFrame, col:str):
    df[col] = df[col].str.title()
    return df

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
complete_rows(col1='anio', col2='grupo_nuevo'),
	rename_cols(map={'grupo_nuevo': 'indicador', 'expo_grupo': 'valor'}),
	replace_na(col='valor', replace_with=0),
	multiplicar_por_escalar(col='valor', k=1e-06),
	str_to_title(col='indicador'),
	sort_values_by_comparison(colname='indicador', precedence={'Oro': 1, 'Plata': 2, 'Litio': 3, 'Cobre': 4, 'Otro': 5}, prefix=['anio'], suffix=[])
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
#  |    |   anio | indicador   |       valor |
#  |---:|-------:|:------------|------------:|
#  |  0 |   1994 | cobre       | 3.63523e+06 |
#  
#  ------------------------------
#  
#  replace_na(col='valor', replace_with=0)
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   indicador  145 non-null    object 
#   2   valor      145 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | Cobre       | 3.63523 |
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
#   2   valor      145 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | Cobre       | 3.63523 |
#  
#  ------------------------------
#  
#  str_to_title(col='indicador')
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   indicador  145 non-null    object 
#   2   valor      145 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1994 | Cobre       | 3.63523 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Oro': 1, 'Plata': 2, 'Litio': 3, 'Cobre': 4, 'Otro': 5}, prefix=['anio'], suffix=[])
#  Index: 145 entries, 2 to 143
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   indicador  145 non-null    object 
#   2   valor      145 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  2 |   1994 | Oro         | 7.09784 |
#  
#  ------------------------------
#  