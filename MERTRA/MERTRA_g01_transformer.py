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
	multiplicar_por_escalar(col='tasa_participacion', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                33 non-null     int64  
#   1   sexo                33 non-null     object 
#   2   tasa_participacion  33 non-null     float64
#  
#  |    |   anio | sexo    |   tasa_participacion |
#  |---:|-------:|:--------|---------------------:|
#  |  0 |   1869 | Varones |             0.906793 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_participacion', k=100)
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                33 non-null     int64  
#   1   sexo                33 non-null     object 
#   2   tasa_participacion  33 non-null     float64
#  
#  |    |   anio | sexo    |   tasa_participacion |
#  |---:|-------:|:--------|---------------------:|
#  |  0 |   1869 | Varones |              90.6793 |
#  
#  ------------------------------
#  