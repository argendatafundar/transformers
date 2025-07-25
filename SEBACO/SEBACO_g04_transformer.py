from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='prop_expo_sbc_servicios', k=100),
	multiplicar_por_escalar(col='prop_expo_sbc_total', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     17 non-null     int64  
#   1   exportaciones_sbc        17 non-null     float64
#   2   prop_expo_sbc_servicios  17 non-null     float64
#   3   prop_expo_sbc_total      17 non-null     float64
#  
#  |    |   anio |   exportaciones_sbc |   prop_expo_sbc_servicios |   prop_expo_sbc_total |
#  |---:|-------:|--------------------:|--------------------------:|----------------------:|
#  |  0 |   2006 |             2521.42 |                  0.318723 |             0.0462404 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop_expo_sbc_servicios', k=100)
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     17 non-null     int64  
#   1   exportaciones_sbc        17 non-null     float64
#   2   prop_expo_sbc_servicios  17 non-null     float64
#   3   prop_expo_sbc_total      17 non-null     float64
#  
#  |    |   anio |   exportaciones_sbc |   prop_expo_sbc_servicios |   prop_expo_sbc_total |
#  |---:|-------:|--------------------:|--------------------------:|----------------------:|
#  |  0 |   2006 |             2521.42 |                   31.8723 |               4.62404 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop_expo_sbc_total', k=100)
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     17 non-null     int64  
#   1   exportaciones_sbc        17 non-null     float64
#   2   prop_expo_sbc_servicios  17 non-null     float64
#   3   prop_expo_sbc_total      17 non-null     float64
#  
#  |    |   anio |   exportaciones_sbc |   prop_expo_sbc_servicios |   prop_expo_sbc_total |
#  |---:|-------:|--------------------:|--------------------------:|----------------------:|
#  |  0 |   2006 |             2521.42 |                   31.8723 |               4.62404 |
#  
#  ------------------------------
#  