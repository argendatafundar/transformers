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
    df[col] = pd.to_datetime(df[col]).dt.year
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador'),
	rename_cols(map={'fecha': 'anio'}),
	datetime_to_year(col='anio')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 172 entries, 0 to 171
#  Data columns (total 4 columns):
#   #   Column                                  Non-Null Count  Dtype  
#  ---  ------                                  --------------  -----  
#   0   fecha                                   172 non-null    object 
#   1   emisiones_anuales_co2_toneladas         172 non-null    float64
#   2   emisiones_anuales_ch4_en_co2_toneladas  172 non-null    float64
#   3   emisiones_anuales_n2o_en_co2_toneladas  172 non-null    float64
#  
#  |    | fecha      |   emisiones_anuales_co2_toneladas |   emisiones_anuales_ch4_en_co2_toneladas |   emisiones_anuales_n2o_en_co2_toneladas |
#  |---:|:-----------|----------------------------------:|-----------------------------------------:|-----------------------------------------:|
#  |  0 | 1850-01-01 |                       2.61643e+09 |                              1.23939e+09 |                              1.47523e+08 |
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
#   2   valor      516 non-null    float64
#  
#  |    | fecha      | indicador                       |       valor |
#  |---:|:-----------|:--------------------------------|------------:|
#  |  0 | 1850-01-01 | emisiones_anuales_co2_toneladas | 2.61643e+09 |
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
#   2   valor      516 non-null    float64
#  
#  |    |   anio | indicador                       |       valor |
#  |---:|-------:|:--------------------------------|------------:|
#  |  0 |   1850 | emisiones_anuales_co2_toneladas | 2.61643e+09 |
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
#   2   valor      516 non-null    float64
#  
#  |    |   anio | indicador                       |       valor |
#  |---:|-------:|:--------------------------------|------------:|
#  |  0 |   1850 | emisiones_anuales_co2_toneladas | 2.61643e+09 |
#  
#  ------------------------------
#  