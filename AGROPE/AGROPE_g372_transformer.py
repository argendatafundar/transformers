from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    data = df.copy()
    data[col] = data[col]*k
    return data
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['productos_primarios', 'moa'], axis=1),
	pivot_longer(id_cols=['anio'], names_to_col='sector', values_to_col='valor'),
	replace_value(col='sector', curr_value='participacion_expo_totales', new_value='Participación en las exportaciones totales'),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 4 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   anio                        14 non-null     int64  
#   1   productos_primarios         14 non-null     float64
#   2   moa                         14 non-null     float64
#   3   participacion_expo_totales  14 non-null     float64
#  
#  |    |   anio |   productos_primarios |   moa |   participacion_expo_totales |
#  |---:|-------:|----------------------:|------:|-----------------------------:|
#  |  0 |   2009 |                  8124 | 21212 |                     0.526943 |
#  
#  ------------------------------
#  
#  drop_col(col=['productos_primarios', 'moa'], axis=1)
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 2 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   anio                        14 non-null     int64  
#   1   participacion_expo_totales  14 non-null     float64
#  
#  |    |   anio |   participacion_expo_totales |
#  |---:|-------:|-----------------------------:|
#  |  0 |   2009 |                     0.526943 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='sector', values_to_col='valor')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    14 non-null     int64  
#   1   sector  14 non-null     object 
#   2   valor   14 non-null     float64
#  
#  |    |   anio | sector                     |    valor |
#  |---:|-------:|:---------------------------|---------:|
#  |  0 |   2009 | participacion_expo_totales | 0.526943 |
#  
#  ------------------------------
#  
#  replace_value(col='sector', curr_value='participacion_expo_totales', new_value='Participación en las exportaciones totales')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    14 non-null     int64  
#   1   sector  14 non-null     object 
#   2   valor   14 non-null     float64
#  
#  |    |   anio | sector                                     |    valor |
#  |---:|-------:|:-------------------------------------------|---------:|
#  |  0 |   2009 | Participación en las exportaciones totales | 0.526943 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    14 non-null     int64  
#   1   sector  14 non-null     object 
#   2   valor   14 non-null     float64
#  
#  |    |   anio | sector                                     |   valor |
#  |---:|-------:|:-------------------------------------------|--------:|
#  |  0 |   2009 | Participación en las exportaciones totales | 52.6943 |
#  
#  ------------------------------
#  