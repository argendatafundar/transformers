from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_na(col='ratio_tasa_actividad_mujer_varon')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16704 entries, 0 to 16703
#  Data columns (total 6 columns):
#   #   Column                            Non-Null Count  Dtype  
#  ---  ------                            --------------  -----  
#   0   geocodigoFundar                   16704 non-null  object 
#   1   geonombreFundar                   16704 non-null  object 
#   2   iso3_desc                         16704 non-null  object 
#   3   anio                              16704 non-null  int64  
#   4   ratio_tasa_actividad_mujer_varon  5027 non-null   float64
#   5   nivel_agregacion                  16704 non-null  object 
#  
#  |    | geocodigoFundar   | geonombreFundar              | iso3_desc                   |   anio |   ratio_tasa_actividad_mujer_varon | nivel_agregacion   |
#  |---:|:------------------|:-----------------------------|:----------------------------|-------:|-----------------------------------:|:-------------------|
#  |  0 | AFE               | África Oriental y Meridional | Africa Eastern and Southern |   2023 |                                nan | agregacion         |
#  
#  ------------------------------
#  
#  drop_na(col='ratio_tasa_actividad_mujer_varon')
#  Index: 5027 entries, 65 to 16681
#  Data columns (total 6 columns):
#   #   Column                            Non-Null Count  Dtype  
#  ---  ------                            --------------  -----  
#   0   geocodigoFundar                   5027 non-null   object 
#   1   geonombreFundar                   5027 non-null   object 
#   2   iso3_desc                         5027 non-null   object 
#   3   anio                              5027 non-null   int64  
#   4   ratio_tasa_actividad_mujer_varon  5027 non-null   float64
#   5   nivel_agregacion                  5027 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar             | iso3_desc                  |   anio |   ratio_tasa_actividad_mujer_varon | nivel_agregacion   |
#  |---:|:------------------|:----------------------------|:---------------------------|-------:|-----------------------------------:|:-------------------|
#  | 65 | AFW               | África Occidental y Central | Africa Western and Central |   2022 |                             87.371 | agregacion         |
#  
#  ------------------------------
#  