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
	drop_na(col='tasa_actividad')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16704 entries, 0 to 16703
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigoFundar   16704 non-null  object 
#   1   geonombreFundar   16704 non-null  object 
#   2   anio              16704 non-null  int64  
#   3   tasa_actividad    7620 non-null   float64
#   4   nivel_agregacion  16704 non-null  object 
#  
#  |    | geocodigoFundar   | geonombreFundar              |   anio |   tasa_actividad | nivel_agregacion   |
#  |---:|:------------------|:-----------------------------|-------:|-----------------:|:-------------------|
#  |  0 | AFE               | África Oriental y Meridional |   2023 |         0.412985 | agregacion         |
#  
#  ------------------------------
#  
#  drop_na(col='tasa_actividad')
#  Index: 7620 entries, 0 to 16672
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigoFundar   7620 non-null   object 
#   1   geonombreFundar   7620 non-null   object 
#   2   anio              7620 non-null   int64  
#   3   tasa_actividad    7620 non-null   float64
#   4   nivel_agregacion  7620 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar              |   anio |   tasa_actividad | nivel_agregacion   |
#  |---:|:------------------|:-----------------------------|-------:|-----------------:|:-------------------|
#  |  0 | AFE               | África Oriental y Meridional |   2023 |         0.412985 | agregacion         |
#  
#  ------------------------------
#  