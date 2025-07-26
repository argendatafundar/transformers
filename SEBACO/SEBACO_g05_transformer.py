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
	multiplicar_por_escalar(col='prop_sbc', k=100.0)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 27 entries, 0 to 26
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      27 non-null     int64  
#   1   sbc       27 non-null     float64
#   2   prop_sbc  27 non-null     float64
#  
#  |    |   anio |    sbc |   prop_sbc |
#  |---:|-------:|-------:|-----------:|
#  |  0 |   1996 | 140069 |   0.039892 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop_sbc', k=100.0)
#  RangeIndex: 27 entries, 0 to 26
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      27 non-null     int64  
#   1   sbc       27 non-null     float64
#   2   prop_sbc  27 non-null     float64
#  
#  |    |   anio |    sbc |   prop_sbc |
#  |---:|-------:|-------:|-----------:|
#  |  0 |   1996 | 140069 |     3.9892 |
#  
#  ------------------------------
#  