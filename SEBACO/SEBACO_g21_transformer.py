from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'prop_sobre_expo_total': 'porc_sobre_expo_total'}),
	multiplicar_por_escalar(col='porc_sobre_expo_total', k=100.0)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   17 non-null     int64  
#   1   exportaciones          17 non-null     float64
#   2   prop_sobre_expo_total  17 non-null     float64
#  
#  |    |   anio |   exportaciones |   prop_sobre_expo_total |
#  |---:|-------:|----------------:|------------------------:|
#  |  0 |   2006 |          338.38 |              0.00620557 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop_sobre_expo_total': 'porc_sobre_expo_total'})
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   17 non-null     int64  
#   1   exportaciones          17 non-null     float64
#   2   porc_sobre_expo_total  17 non-null     float64
#  
#  |    |   anio |   exportaciones |   porc_sobre_expo_total |
#  |---:|-------:|----------------:|------------------------:|
#  |  0 |   2006 |          338.38 |                0.620557 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='porc_sobre_expo_total', k=100.0)
#  RangeIndex: 17 entries, 0 to 16
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   17 non-null     int64  
#   1   exportaciones          17 non-null     float64
#   2   porc_sobre_expo_total  17 non-null     float64
#  
#  |    |   anio |   exportaciones |   porc_sobre_expo_total |
#  |---:|-------:|----------------:|------------------------:|
#  |  0 |   2006 |          338.38 |                0.620557 |
#  
#  ------------------------------
#  