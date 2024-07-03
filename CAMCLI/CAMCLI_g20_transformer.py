from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def datetime_to_year(df, col: str):
    import pandas as pd
    
    df[col] = pd.to_datetime(df[col]).dt.year
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def promedio_anual(df: pd.DataFrame):
    return df.groupby(['anio', 'indicador']).agg({'valor': 'mean'}).reset_index()
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador'),
	rename_cols(map={'fecha': 'anio'}),
	datetime_to_year(col='anio'),
	replace_value(col='indicador', curr_value='anomalia_temperatura_mar_relativ', new_value='Mar'),
	replace_value(col='indicador', curr_value='anomalia_temperatura_tierra_relativ', new_value='Tierra'),
	promedio_anual()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2088 entries, 0 to 2087
#  Data columns (total 3 columns):
#   #   Column                               Non-Null Count  Dtype  
#  ---  ------                               --------------  -----  
#   0   fecha                                2088 non-null   object 
#   1   anomalia_temperatura_tierra_relativ  2086 non-null   float64
#   2   anomalia_temperatura_mar_relativ     2088 non-null   float64
#  
#  |    | fecha      |   anomalia_temperatura_tierra_relativ |   anomalia_temperatura_mar_relativ |
#  |---:|:-----------|--------------------------------------:|-----------------------------------:|
#  |  0 | 1850-01-01 |                             -0.214667 |                         -0.0916589 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   fecha      4176 non-null   object 
#   1   indicador  4176 non-null   object 
#   2   valor      4174 non-null   float64
#  
#  |    | fecha      | indicador                           |     valor |
#  |---:|:-----------|:------------------------------------|----------:|
#  |  0 | 1850-01-01 | anomalia_temperatura_tierra_relativ | -0.214667 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fecha': 'anio'})
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4176 non-null   int32  
#   1   indicador  4176 non-null   object 
#   2   valor      4174 non-null   float64
#  
#  |    |   anio | indicador                           |     valor |
#  |---:|-------:|:------------------------------------|----------:|
#  |  0 |   1850 | anomalia_temperatura_tierra_relativ | -0.214667 |
#  
#  ------------------------------
#  
#  datetime_to_year(col='anio')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4176 non-null   int32  
#   1   indicador  4176 non-null   object 
#   2   valor      4174 non-null   float64
#  
#  |    |   anio | indicador                           |     valor |
#  |---:|-------:|:------------------------------------|----------:|
#  |  0 |   1850 | anomalia_temperatura_tierra_relativ | -0.214667 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='anomalia_temperatura_mar_relativ', new_value='Mar')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4176 non-null   int32  
#   1   indicador  4176 non-null   object 
#   2   valor      4174 non-null   float64
#  
#  |    |   anio | indicador                           |     valor |
#  |---:|-------:|:------------------------------------|----------:|
#  |  0 |   1850 | anomalia_temperatura_tierra_relativ | -0.214667 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='anomalia_temperatura_tierra_relativ', new_value='Tierra')
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4176 non-null   int32  
#   1   indicador  4176 non-null   object 
#   2   valor      4174 non-null   float64
#  
#  |    |   anio | indicador   |     valor |
#  |---:|-------:|:------------|----------:|
#  |  0 |   1850 | Tierra      | -0.214667 |
#  
#  ------------------------------
#  
#  promedio_anual()
#  RangeIndex: 348 entries, 0 to 347
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       348 non-null    int32  
#   1   indicador  348 non-null    object 
#   2   valor      348 non-null    float64
#  
#  |    |   anio | indicador   |      valor |
#  |---:|-------:|:------------|-----------:|
#  |  0 |   1850 | Mar         | -0.0497822 |
#  
#  ------------------------------
#  