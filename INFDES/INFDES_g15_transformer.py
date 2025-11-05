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
	multiplicar_por_escalar(col='tasa_desocupacion', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               84 non-null     int64  
#   1   nivel_ed_cod       84 non-null     int64  
#   2   nivel_ed_desc      84 non-null     object 
#   3   tasa_desocupacion  84 non-null     float64
#  
#  |    |   anio |   nivel_ed_cod | nivel_ed_desc               |   tasa_desocupacion |
#  |---:|-------:|---------------:|:----------------------------|--------------------:|
#  |  0 |   2003 |              1 | Hasta secundaria incompleta |            0.119657 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_desocupacion', k=100)
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               84 non-null     int64  
#   1   nivel_ed_cod       84 non-null     int64  
#   2   nivel_ed_desc      84 non-null     object 
#   3   tasa_desocupacion  84 non-null     float64
#  
#  |    |   anio |   nivel_ed_cod | nivel_ed_desc               |   tasa_desocupacion |
#  |---:|-------:|---------------:|:----------------------------|--------------------:|
#  |  0 |   2003 |              1 | Hasta secundaria incompleta |             11.9657 |
#  
#  ------------------------------
#  