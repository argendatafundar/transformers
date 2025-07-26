from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='vbp_ssi_2022', k=0.001),
	multiplicar_por_escalar(col='prop_ssi_total', k=100.0),
	drop_col(col='prop_ssi_total', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            20 non-null     int64  
#   1   vbp_ssi_2022    20 non-null     int64  
#   2   prop_ssi_total  19 non-null     float64
#  
#  |    |   anio |   vbp_ssi_2022 |   prop_ssi_total |
#  |---:|-------:|---------------:|-----------------:|
#  |  0 |   2003 |         348265 |              nan |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='vbp_ssi_2022', k=0.001)
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            20 non-null     int64  
#   1   vbp_ssi_2022    20 non-null     float64
#   2   prop_ssi_total  19 non-null     float64
#  
#  |    |   anio |   vbp_ssi_2022 |   prop_ssi_total |
#  |---:|-------:|---------------:|-----------------:|
#  |  0 |   2003 |        348.265 |              nan |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop_ssi_total', k=100.0)
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            20 non-null     int64  
#   1   vbp_ssi_2022    20 non-null     float64
#   2   prop_ssi_total  19 non-null     float64
#  
#  |    |   anio |   vbp_ssi_2022 |   prop_ssi_total |
#  |---:|-------:|---------------:|-----------------:|
#  |  0 |   2003 |        348.265 |              nan |
#  
#  ------------------------------
#  
#  drop_col(col='prop_ssi_total', axis=1)
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 2 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          20 non-null     int64  
#   1   vbp_ssi_2022  20 non-null     float64
#  
#  |    |   anio |   vbp_ssi_2022 |
#  |---:|-------:|---------------:|
#  |  0 |   2003 |        348.265 |
#  
#  ------------------------------
#  