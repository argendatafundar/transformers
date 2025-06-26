from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: pl.DataFrame) -> pl.DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 6 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   geocodigoFundar              24 non-null     object 
#   1   geonombreFundar              24 non-null     object 
#   2   region_pbg                   24 non-null     object 
#   3   pib_pc_1895                  24 non-null     float64
#   4   pib_pc_ultimo_anio           24 non-null     float64
#   5   var_pib_pc_1895_ultimo_anio  24 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | region_pbg      |   pib_pc_1895 |   pib_pc_ultimo_anio |   var_pib_pc_1895_ultimo_anio |
#  |---:|:------------------|:------------------|:----------------|--------------:|---------------------:|------------------------------:|
#  |  0 | AR-B              | Buenos Aires      | Pampeana y AMBA |       4413.34 |              10992.4 |                       1.49071 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 6 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   geocodigoFundar              24 non-null     object 
#   1   geonombreFundar              24 non-null     object 
#   2   region_pbg                   24 non-null     object 
#   3   pib_pc_1895                  24 non-null     float64
#   4   pib_pc_ultimo_anio           24 non-null     float64
#   5   var_pib_pc_1895_ultimo_anio  24 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | region_pbg      |   pib_pc_1895 |   pib_pc_ultimo_anio |   var_pib_pc_1895_ultimo_anio |
#  |---:|:------------------|:------------------|:----------------|--------------:|---------------------:|------------------------------:|
#  |  0 | AR-B              | Buenos Aires      | Pampeana y AMBA |       4413.34 |              10992.4 |                       1.49071 |
#  
#  ------------------------------
#  