from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['geocodigoFundar', 'pib_pc_ultimo_anio'], axis=1),
	wide_to_long(primary_keys=['geonombreFundar', 'region_pbg'], value_name='valor', var_name='indicador')
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
#  drop_col(col=['geocodigoFundar', 'pib_pc_ultimo_anio'], axis=1)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 4 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   geonombreFundar              24 non-null     object 
#   1   region_pbg                   24 non-null     object 
#   2   pib_pc_1895                  24 non-null     float64
#   3   var_pib_pc_1895_ultimo_anio  24 non-null     float64
#  
#  |    | geonombreFundar   | region_pbg      |   pib_pc_1895 |   var_pib_pc_1895_ultimo_anio |
#  |---:|:------------------|:----------------|--------------:|------------------------------:|
#  |  0 | Buenos Aires      | Pampeana y AMBA |       4413.34 |                       1.49071 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geonombreFundar', 'region_pbg'], value_name='valor', var_name='indicador')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  48 non-null     object 
#   1   region_pbg       48 non-null     object 
#   2   indicador        48 non-null     object 
#   3   valor            48 non-null     float64
#  
#  |    | geonombreFundar   | region_pbg      | indicador   |   valor |
#  |---:|:------------------|:----------------|:------------|--------:|
#  |  0 | Buenos Aires      | Pampeana y AMBA | pib_pc_1895 | 4413.34 |
#  
#  ------------------------------
#  