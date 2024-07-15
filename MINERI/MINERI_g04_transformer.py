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
def str_to_title(df: DataFrame, col:str):
    df[col] = df[col].str.title()
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'grupo_nuevo': 'indicador', 'expo_grupo': 'valor'}),
	multiplicar_por_escalar(col='valor', k=1e-06),
	str_to_title(col='indicador'),
	sort_values(how='ascending', by=['anio', 'indicador'])
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
#  rename_cols(map={'grupo_nuevo': 'indicador', 'expo_grupo': 'valor'})
#  RangeIndex: 139 entries, 0 to 138
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  139 non-null    object 
#   1   anio       139 non-null    int64  
#   2   valor      139 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Cobre       |   1994 | 3.63523 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  RangeIndex: 139 entries, 0 to 138
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  139 non-null    object 
#   1   anio       139 non-null    int64  
#   2   valor      139 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Cobre       |   1994 | 3.63523 |
#  
#  ------------------------------
#  
#  str_to_title(col='indicador')
#  RangeIndex: 139 entries, 0 to 138
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  139 non-null    object 
#   1   anio       139 non-null    int64  
#   2   valor      139 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Cobre       |   1994 | 3.63523 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'indicador'])
#  RangeIndex: 139 entries, 0 to 138
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  139 non-null    object 
#   1   anio       139 non-null    int64  
#   2   valor      139 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Cobre       |   1994 | 3.63523 |
#  
#  ------------------------------
#  