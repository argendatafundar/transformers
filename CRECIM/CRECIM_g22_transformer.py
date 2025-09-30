from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='var_pib_pc_1895_ultimo_anio', k=100),
	drop_col(col=['geocodigoFundar', 'pib_pc_ultimo_anio'], axis=1),
	wide_to_long(primary_keys=['geonombreFundar', 'region_pbg'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='pib_pc_1895', new_value='PIB per cápita de 1895 (en pesos de 2004)', mapping=None),
	replace_value(col='indicador', curr_value='var_pib_pc_1895_ultimo_anio', new_value='Variación del PIB per cápita entre 1895 y 2022', mapping=None)
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
#  multiplicar_por_escalar(col='var_pib_pc_1895_ultimo_anio', k=100)
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
#  |  0 | AR-B              | Buenos Aires      | Pampeana y AMBA |       4413.34 |              10992.4 |                       149.071 |
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
#  |  0 | Buenos Aires      | Pampeana y AMBA |       4413.34 |                       149.071 |
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
#  replace_value(col='indicador', curr_value='pib_pc_1895', new_value='PIB per cápita de 1895 (en pesos de 2004)', mapping=None)
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  48 non-null     object 
#   1   region_pbg       48 non-null     object 
#   2   indicador        48 non-null     object 
#   3   valor            48 non-null     float64
#  
#  |    | geonombreFundar   | region_pbg      | indicador                                 |   valor |
#  |---:|:------------------|:----------------|:------------------------------------------|--------:|
#  |  0 | Buenos Aires      | Pampeana y AMBA | PIB per cápita de 1895 (en pesos de 2004) | 4413.34 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='var_pib_pc_1895_ultimo_anio', new_value='Variación del PIB per cápita entre 1895 y 2022', mapping=None)
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  48 non-null     object 
#   1   region_pbg       48 non-null     object 
#   2   indicador        48 non-null     object 
#   3   valor            48 non-null     float64
#  
#  |    | geonombreFundar   | region_pbg      | indicador                                 |   valor |
#  |---:|:------------------|:----------------|:------------------------------------------|--------:|
#  |  0 | Buenos Aires      | Pampeana y AMBA | PIB per cápita de 1895 (en pesos de 2004) | 4413.34 |
#  
#  ------------------------------
#  