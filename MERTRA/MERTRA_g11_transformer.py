from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def imput_na(df:DataFrame, bool_mask:list[bool], col:str, value:Any):
    df.loc[bool_mask,col] = value
    return df

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

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	imput_na(bool_mask=[False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False], col='geonombreFundar', value='Argentina'),
	query(condition="geonombreFundar != 'Argentina'"),
	query(condition='anio == anio.max()'),
	drop_col(col=['geocodigoFundar', 'anio'], axis=1),
	pivot_longer(id_cols=['geonombreFundar'], names_to_col='variable', values_to_col='valor'),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 197 entries, 0 to 196
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  189 non-null    object 
#   1   geonombreFundar  189 non-null    object 
#   2   anio             197 non-null    int64  
#   3   tasa_empleo      197 non-null    float64
#   4   tasa_menor_18    197 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   tasa_empleo |   tasa_menor_18 |
#  |---:|:------------------|:------------------|-------:|--------------:|----------------:|
#  |  0 | AR-B              | Buenos Aires      |   2016 |      0.406335 |        0.282782 |
#  
#  ------------------------------
#  
#  imput_na(bool_mask=[False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, False, True, False], col='geonombreFundar', value='Argentina')
#  RangeIndex: 197 entries, 0 to 196
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  189 non-null    object 
#   1   geonombreFundar  197 non-null    object 
#   2   anio             197 non-null    int64  
#   3   tasa_empleo      197 non-null    float64
#   4   tasa_menor_18    197 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   tasa_empleo |   tasa_menor_18 |
#  |---:|:------------------|:------------------|-------:|--------------:|----------------:|
#  |  0 | AR-B              | Buenos Aires      |   2016 |      0.406335 |        0.282782 |
#  
#  ------------------------------
#  
#  query(condition="geonombreFundar != 'Argentina'")
#  Index: 189 entries, 0 to 196
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  189 non-null    object 
#   1   geonombreFundar  189 non-null    object 
#   2   anio             189 non-null    int64  
#   3   tasa_empleo      189 non-null    float64
#   4   tasa_menor_18    189 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   tasa_empleo |   tasa_menor_18 |
#  |---:|:------------------|:------------------|-------:|--------------:|----------------:|
#  |  0 | AR-B              | Buenos Aires      |   2016 |      0.406335 |        0.282782 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 24 entries, 172 to 196
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  24 non-null     object 
#   1   geonombreFundar  24 non-null     object 
#   2   anio             24 non-null     int64  
#   3   tasa_empleo      24 non-null     float64
#   4   tasa_menor_18    24 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio |   tasa_empleo |   tasa_menor_18 |
#  |----:|:------------------|:------------------|-------:|--------------:|----------------:|
#  | 172 | AR-B              | Buenos Aires      |   2023 |      0.450225 |         0.28205 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'anio'], axis=1)
#  Index: 24 entries, 172 to 196
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  24 non-null     object 
#   1   tasa_empleo      24 non-null     float64
#   2   tasa_menor_18    24 non-null     float64
#  
#  |     | geonombreFundar   |   tasa_empleo |   tasa_menor_18 |
#  |----:|:------------------|--------------:|----------------:|
#  | 172 | Buenos Aires      |      0.450225 |         0.28205 |
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
#  |    | geonombreFundar   | variable    |   valor |
#  |---:|:------------------|:------------|--------:|
#  |  0 | Buenos Aires      | tasa_empleo | 45.0225 |
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
#  |    | geonombreFundar   | variable    |   valor |
#  |---:|:------------------|:------------|--------:|
#  |  0 | Buenos Aires      | tasa_empleo | 45.0225 |
#  
#  ------------------------------
#  