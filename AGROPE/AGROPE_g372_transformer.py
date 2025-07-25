from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['pp_sin_mineria', 'moa', 'total'], axis=1),
	pivot_longer(id_cols=['anio'], names_to_col='sector', values_to_col='valor'),
	replace_value(col='sector', curr_value='participacion_expo_totales', new_value='Participación en las exportaciones totales'),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 5 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   anio                        33 non-null     int64  
#   1   moa                         33 non-null     float64
#   2   pp_sin_mineria              33 non-null     float64
#   3   total                       33 non-null     float64
#   4   participacion_expo_totales  33 non-null     float64
#  
#  |    |   anio |     moa |   pp_sin_mineria |   total |   participacion_expo_totales |
#  |---:|-------:|--------:|-----------------:|--------:|-----------------------------:|
#  |  0 |   1992 | 4837.11 |          3474.69 | 12234.9 |                     0.679349 |
#  
#  ------------------------------
#  
#  drop_col(col=['pp_sin_mineria', 'moa', 'total'], axis=1)
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 2 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   anio                        33 non-null     int64  
#   1   participacion_expo_totales  33 non-null     float64
#  
#  |    |   anio |   participacion_expo_totales |
#  |---:|-------:|-----------------------------:|
#  |  0 |   1992 |                     0.679349 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='sector', values_to_col='valor')
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    33 non-null     int64  
#   1   sector  33 non-null     object 
#   2   valor   33 non-null     float64
#  
#  |    |   anio | sector                     |    valor |
#  |---:|-------:|:---------------------------|---------:|
#  |  0 |   1992 | participacion_expo_totales | 0.679349 |
#  
#  ------------------------------
#  
#  replace_value(col='sector', curr_value='participacion_expo_totales', new_value='Participación en las exportaciones totales')
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    33 non-null     int64  
#   1   sector  33 non-null     object 
#   2   valor   33 non-null     float64
#  
#  |    |   anio | sector                                     |   valor |
#  |---:|-------:|:-------------------------------------------|--------:|
#  |  0 |   1992 | Participación en las exportaciones totales | 67.9349 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    33 non-null     int64  
#   1   sector  33 non-null     object 
#   2   valor   33 non-null     float64
#  
#  |    |   anio | sector                                     |   valor |
#  |---:|-------:|:-------------------------------------------|--------:|
#  |  0 |   1992 | Participación en las exportaciones totales | 67.9349 |
#  
#  ------------------------------
#  