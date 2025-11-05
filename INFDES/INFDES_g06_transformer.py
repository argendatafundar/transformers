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
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 394 entries, 0 to 393
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    378 non-null    object 
#   1   geonombreFundar    378 non-null    object 
#   2   anio               394 non-null    int64  
#   3   prov_cod           394 non-null    int64  
#   4   tipo_informalidad  394 non-null    object 
#   5   valor              394 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   prov_cod | tipo_informalidad                    |    valor |
#  |---:|:------------------|:------------------|-------:|-----------:|:-------------------------------------|---------:|
#  |  0 | AR-C              | CABA              |   2016 |          2 | Informalidad (definición productiva) | 0.262605 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 394 entries, 0 to 393
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    378 non-null    object 
#   1   geonombreFundar    378 non-null    object 
#   2   anio               394 non-null    int64  
#   3   prov_cod           394 non-null    int64  
#   4   tipo_informalidad  394 non-null    object 
#   5   valor              394 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   prov_cod | tipo_informalidad                    |   valor |
#  |---:|:------------------|:------------------|-------:|-----------:|:-------------------------------------|--------:|
#  |  0 | AR-C              | CABA              |   2016 |          2 | Informalidad (definición productiva) | 26.2605 |
#  
#  ------------------------------
#  