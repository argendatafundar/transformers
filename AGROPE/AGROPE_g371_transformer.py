from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['participacion_expo_totales'], axis=1),
	pivot_longer(id_cols=['anio'], names_to_col='sector', values_to_col='valor'),
	replace_value(col='sector', curr_value='productos_primarios', new_value='Productos primarios'),
	replace_value(col='sector', curr_value='moa', new_value='MOA')
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
#  drop_col(col=['participacion_expo_totales'], axis=1)
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 14 non-null     int64  
#   1   productos_primarios  14 non-null     float64
#   2   moa                  14 non-null     float64
#  
#  |    |   anio |   productos_primarios |   moa |
#  |---:|-------:|----------------------:|------:|
#  |  0 |   2009 |                  8124 | 21212 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='sector', values_to_col='valor')
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    28 non-null     int64  
#   1   sector  28 non-null     object 
#   2   valor   28 non-null     float64
#  
#  |    |   anio | sector              |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   2009 | productos_primarios |    8124 |
#  
#  ------------------------------
#  
#  replace_value(col='sector', curr_value='productos_primarios', new_value='Productos primarios')
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    28 non-null     int64  
#   1   sector  28 non-null     object 
#   2   valor   28 non-null     float64
#  
#  |    |   anio | sector              |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   2009 | Productos primarios |    8124 |
#  
#  ------------------------------
#  
#  replace_value(col='sector', curr_value='moa', new_value='MOA')
#  RangeIndex: 28 entries, 0 to 27
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    28 non-null     int64  
#   1   sector  28 non-null     object 
#   2   valor   28 non-null     float64
#  
#  |    |   anio | sector              |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   2009 | Productos primarios |    8124 |
#  
#  ------------------------------
#  