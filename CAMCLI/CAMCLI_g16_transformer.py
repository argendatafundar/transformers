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
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def share_valor(df:DataFrame, group_col:str, measure_col:str): 
    df['share_tipo_gas'] = df.groupby(group_col)[measure_col].transform(lambda x: 100* x/ sum(x))
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador'),
	rename_cols(map={'fecha': 'anio'}),
	datetime_to_year(col='anio'),
	replace_value(col='indicador', curr_value='emisiones_anuales_co2_toneladas', new_value='Dióxido de carbono (CO2)'),
	replace_value(col='indicador', curr_value='emisiones_anuales_n2o_en_co2_toneladas', new_value='Óxido nitroso (N2O)'),
	replace_value(col='indicador', curr_value='emisiones_anuales_ch4_en_co2_toneladas', new_value='Metano (CH4)'),
	sort_values(how='ascending', by=['anio']),
	share_valor(group_col='anio', measure_col='valor'),
	drop_col(col='valor', axis=1),
	rename_cols(map={'share_tipo_gas': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 172 entries, 0 to 171
#  Data columns (total 4 columns):
#   #   Column                                  Non-Null Count  Dtype 
#  ---  ------                                  --------------  ----- 
#   0   fecha                                   172 non-null    object
#   1   emisiones_anuales_co2_toneladas         172 non-null    int64 
#   2   emisiones_anuales_ch4_en_co2_toneladas  172 non-null    int64 
#   3   emisiones_anuales_n2o_en_co2_toneladas  172 non-null    int64 
#  
#  |    | fecha      |   emisiones_anuales_co2_toneladas |   emisiones_anuales_ch4_en_co2_toneladas |   emisiones_anuales_n2o_en_co2_toneladas |
#  |---:|:-----------|----------------------------------:|-----------------------------------------:|-----------------------------------------:|
#  |  0 | 1850-01-01 |                        2839368700 |                               1239410300 |                                138085570 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador')
#  RangeIndex: 516 entries, 0 to 515
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   fecha      516 non-null    object
#   1   indicador  516 non-null    object
#   2   valor      516 non-null    int64 
#  
#  |    | fecha      | indicador                       |      valor |
#  |---:|:-----------|:--------------------------------|-----------:|
#  |  0 | 1850-01-01 | emisiones_anuales_co2_toneladas | 2839368700 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fecha': 'anio'})
#  RangeIndex: 516 entries, 0 to 515
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       516 non-null    int32 
#   1   indicador  516 non-null    object
#   2   valor      516 non-null    int64 
#  
#  |    |   anio | indicador                       |      valor |
#  |---:|-------:|:--------------------------------|-----------:|
#  |  0 |   1850 | emisiones_anuales_co2_toneladas | 2839368700 |
#  
#  ------------------------------
#  
#  datetime_to_year(col='anio')
#  RangeIndex: 516 entries, 0 to 515
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       516 non-null    int32 
#   1   indicador  516 non-null    object
#   2   valor      516 non-null    int64 
#  
#  |    |   anio | indicador                       |      valor |
#  |---:|-------:|:--------------------------------|-----------:|
#  |  0 |   1850 | emisiones_anuales_co2_toneladas | 2839368700 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='emisiones_anuales_co2_toneladas', new_value='Dióxido de carbono (CO2)')
#  RangeIndex: 516 entries, 0 to 515
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       516 non-null    int32 
#   1   indicador  516 non-null    object
#   2   valor      516 non-null    int64 
#  
#  |    |   anio | indicador                |      valor |
#  |---:|-------:|:-------------------------|-----------:|
#  |  0 |   1850 | Dióxido de carbono (CO2) | 2839368700 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='emisiones_anuales_n2o_en_co2_toneladas', new_value='Óxido nitroso (N2O)')
#  RangeIndex: 516 entries, 0 to 515
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       516 non-null    int32 
#   1   indicador  516 non-null    object
#   2   valor      516 non-null    int64 
#  
#  |    |   anio | indicador                |      valor |
#  |---:|-------:|:-------------------------|-----------:|
#  |  0 |   1850 | Dióxido de carbono (CO2) | 2839368700 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='emisiones_anuales_ch4_en_co2_toneladas', new_value='Metano (CH4)')
#  RangeIndex: 516 entries, 0 to 515
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       516 non-null    int32 
#   1   indicador  516 non-null    object
#   2   valor      516 non-null    int64 
#  
#  |    |   anio | indicador                |      valor |
#  |---:|-------:|:-------------------------|-----------:|
#  |  0 |   1850 | Dióxido de carbono (CO2) | 2839368700 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 516 entries, 0 to 515
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            516 non-null    int32  
#   1   indicador       516 non-null    object 
#   2   valor           516 non-null    int64  
#   3   share_tipo_gas  516 non-null    float64
#  
#  |    |   anio | indicador                |      valor |   share_tipo_gas |
#  |---:|-------:|:-------------------------|-----------:|-----------------:|
#  |  0 |   1850 | Dióxido de carbono (CO2) | 2839368700 |          67.3336 |
#  
#  ------------------------------
#  
#  share_valor(group_col='anio', measure_col='valor')
#  RangeIndex: 516 entries, 0 to 515
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            516 non-null    int32  
#   1   indicador       516 non-null    object 
#   2   valor           516 non-null    int64  
#   3   share_tipo_gas  516 non-null    float64
#  
#  |    |   anio | indicador                |      valor |   share_tipo_gas |
#  |---:|-------:|:-------------------------|-----------:|-----------------:|
#  |  0 |   1850 | Dióxido de carbono (CO2) | 2839368700 |          67.3336 |
#  
#  ------------------------------
#  
#  drop_col(col='valor', axis=1)
#  RangeIndex: 516 entries, 0 to 515
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            516 non-null    int32  
#   1   indicador       516 non-null    object 
#   2   share_tipo_gas  516 non-null    float64
#  
#  |    |   anio | indicador                |   share_tipo_gas |
#  |---:|-------:|:-------------------------|-----------------:|
#  |  0 |   1850 | Dióxido de carbono (CO2) |          67.3336 |
#  
#  ------------------------------
#  
#  rename_cols(map={'share_tipo_gas': 'valor'})
#  RangeIndex: 516 entries, 0 to 515
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       516 non-null    int32  
#   1   indicador  516 non-null    object 
#   2   valor      516 non-null    float64
#  
#  |    |   anio | indicador                |   valor |
#  |---:|-------:|:-------------------------|--------:|
#  |  0 |   1850 | Dióxido de carbono (CO2) | 67.3336 |
#  
#  ------------------------------
#  