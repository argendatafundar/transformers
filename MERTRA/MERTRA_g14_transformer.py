from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

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
	drop_col(col=['geocodigoFundar', 'prov_cod'], axis=1),
	pivot_longer(id_cols=['geonombreFundar'], names_to_col='variable', values_to_col='valor'),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   geocodigoFundar            24 non-null     object 
#   1   geonombreFundar            24 non-null     object 
#   2   prov_cod                   24 non-null     int64  
#   3   tasa_empleo_18_65_mujeres  24 non-null     float64
#   4   prop_usa_lavarropas        24 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   prov_cod |   tasa_empleo_18_65_mujeres |   prop_usa_lavarropas |
#  |---:|:------------------|:------------------|-----------:|----------------------------:|----------------------:|
#  |  0 | AR-C              | CABA              |          2 |                     0.75518 |              0.766963 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'prov_cod'], axis=1)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   geonombreFundar            24 non-null     object 
#   1   tasa_empleo_18_65_mujeres  24 non-null     float64
#   2   prop_usa_lavarropas        24 non-null     float64
#  
#  |    | geonombreFundar   |   tasa_empleo_18_65_mujeres |   prop_usa_lavarropas |
#  |---:|:------------------|----------------------------:|----------------------:|
#  |  0 | CABA              |                     0.75518 |              0.766963 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['geonombreFundar'], names_to_col='variable', values_to_col='valor')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  48 non-null     object 
#   1   variable         48 non-null     object 
#   2   valor            48 non-null     float64
#  
#  |    | geonombreFundar   | variable                  |   valor |
#  |---:|:------------------|:--------------------------|--------:|
#  |  0 | CABA              | tasa_empleo_18_65_mujeres |  75.518 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  48 non-null     object 
#   1   variable         48 non-null     object 
#   2   valor            48 non-null     float64
#  
#  |    | geonombreFundar   | variable                  |   valor |
#  |---:|:------------------|:--------------------------|--------:|
#  |  0 | CABA              | tasa_empleo_18_65_mujeres |  75.518 |
#  
#  ------------------------------
#  