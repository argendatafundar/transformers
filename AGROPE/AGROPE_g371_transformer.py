from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

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
	drop_col(col=['participacion_expo_totales', 'total'], axis=1),
	pivot_longer(id_cols=['anio'], names_to_col='sector', values_to_col='valor'),
	replace_value(col='sector', curr_value='pp_sin_mineria', new_value='Productos primarios'),
	replace_value(col='sector', curr_value='moa', new_value='MOA'),
	query(condition='anio >= 2009')
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
#  drop_col(col=['participacion_expo_totales', 'total'], axis=1)
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            33 non-null     int64  
#   1   moa             33 non-null     float64
#   2   pp_sin_mineria  33 non-null     float64
#  
#  |    |   anio |     moa |   pp_sin_mineria |
#  |---:|-------:|--------:|-----------------:|
#  |  0 |   1992 | 4837.11 |          3474.69 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='sector', values_to_col='valor')
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    66 non-null     int64  
#   1   sector  66 non-null     object 
#   2   valor   66 non-null     float64
#  
#  |    |   anio | sector   |   valor |
#  |---:|-------:|:---------|--------:|
#  |  0 |   1992 | moa      | 4837.11 |
#  
#  ------------------------------
#  
#  replace_value(col='sector', curr_value='pp_sin_mineria', new_value='Productos primarios')
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    66 non-null     int64  
#   1   sector  66 non-null     object 
#   2   valor   66 non-null     float64
#  
#  |    |   anio | sector   |   valor |
#  |---:|-------:|:---------|--------:|
#  |  0 |   1992 | moa      | 4837.11 |
#  
#  ------------------------------
#  
#  replace_value(col='sector', curr_value='moa', new_value='MOA')
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    66 non-null     int64  
#   1   sector  66 non-null     object 
#   2   valor   66 non-null     float64
#  
#  |    |   anio | sector   |   valor |
#  |---:|-------:|:---------|--------:|
#  |  0 |   1992 | MOA      | 4837.11 |
#  
#  ------------------------------
#  
#  query(condition='anio >= 2009')
#  Index: 32 entries, 17 to 65
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    32 non-null     int64  
#   1   sector  32 non-null     object 
#   2   valor   32 non-null     float64
#  
#  |    |   anio | sector   |   valor |
#  |---:|-------:|:---------|--------:|
#  | 17 |   2009 | MOA      | 21224.8 |
#  
#  ------------------------------
#  