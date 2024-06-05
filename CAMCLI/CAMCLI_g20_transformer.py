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
#  RangeIndex: 2089 entries, 0 to 2088
#  Data columns (total 3 columns):
#   #   Column                               Non-Null Count  Dtype  
#  ---  ------                               --------------  -----  
#   0   fecha                                2089 non-null   object 
#   1   anomalia_temperatura_mar_relativ     2084 non-null   float64
#   2   anomalia_temperatura_tierra_relativ  2083 non-null   float64
#  
#  |    | fecha      |   anomalia_temperatura_mar_relativ |   anomalia_temperatura_tierra_relativ |
#  |---:|:-----------|-----------------------------------:|--------------------------------------:|
#  |  0 | 1850-01-01 |                         -0.0714867 |                             -0.292516 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador')
#  RangeIndex: 4178 entries, 0 to 4177
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   fecha      4178 non-null   object 
#   1   indicador  4178 non-null   object 
#   2   valor      4167 non-null   float64
#  
#  |    | fecha      | indicador                        |      valor |
#  |---:|:-----------|:---------------------------------|-----------:|
#  |  0 | 1850-01-01 | anomalia_temperatura_mar_relativ | -0.0714867 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fecha': 'anio'})
#  RangeIndex: 4178 entries, 0 to 4177
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4178 non-null   int32  
#   1   indicador  4178 non-null   object 
#   2   valor      4167 non-null   float64
#  
#  |    |   anio | indicador                        |      valor |
#  |---:|-------:|:---------------------------------|-----------:|
#  |  0 |   1850 | anomalia_temperatura_mar_relativ | -0.0714867 |
#  
#  ------------------------------
#  
#  datetime_to_year(col='anio')
#  RangeIndex: 4178 entries, 0 to 4177
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4178 non-null   int32  
#   1   indicador  4178 non-null   object 
#   2   valor      4167 non-null   float64
#  
#  |    |   anio | indicador                        |      valor |
#  |---:|-------:|:---------------------------------|-----------:|
#  |  0 |   1850 | anomalia_temperatura_mar_relativ | -0.0714867 |
#  
#  ------------------------------
#  