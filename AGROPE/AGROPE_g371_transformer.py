from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
	drop_col(col=['productos_primarios', 'moa'], axis=1),
	rename_cols(map={'participacion_expo_totales': 'valor'})
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
#  rename_cols(map={'participacion_expo_totales': 'valor'})
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    14 non-null     int64  
#   1   valor   14 non-null     float64
#  
#  |    |   anio |    valor |
#  |---:|-------:|---------:|
#  |  0 |   2009 | 0.526943 |
#  
#  ------------------------------
#  