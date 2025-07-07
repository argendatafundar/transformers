from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['geocodigoFundar', 'ultimo_anio_disponible', 'fuente'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 57 entries, 0 to 56
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         57 non-null     object 
#   1   geonombreFundar         57 non-null     object 
#   2   ultimo_anio_disponible  57 non-null     int64  
#   3   ejc_pea_1000            57 non-null     float64
#   4   fuente                  57 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ultimo_anio_disponible |   ejc_pea_1000 | fuente   |
#  |---:|:------------------|:------------------|-------------------------:|---------------:|:---------|
#  |  0 | DNK               | Dinamarca         |                     2023 |        16.9399 | OECD     |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'ultimo_anio_disponible', 'fuente'], axis=1)
#  RangeIndex: 57 entries, 0 to 56
#  Data columns (total 2 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  57 non-null     object 
#   1   ejc_pea_1000     57 non-null     float64
#  
#  |    | geonombreFundar   |   ejc_pea_1000 |
#  |---:|:------------------|---------------:|
#  |  0 | Dinamarca         |        16.9399 |
#  
#  ------------------------------
#  