from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	drop_col(col=['geocodigoFundar', 'anio'], axis=1),
	multiplicar_por_escalar(col='tasa_empleo_mujer', k=100),
	pivot_longer(id_cols=['geonombreFundar'], names_to_col='variable', values_to_col='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 189 entries, 0 to 188
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    189 non-null    object 
#   1   geonombreFundar    189 non-null    object 
#   2   anio               189 non-null    int64  
#   3   salario_relativo   189 non-null    float64
#   4   tasa_empleo_mujer  189 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   salario_relativo |   tasa_empleo_mujer |
#  |---:|:------------------|:------------------|-------:|-------------------:|--------------------:|
#  |  0 | AR-B              | Buenos Aires      |   2016 |            98.1688 |            0.542051 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 24 entries, 165 to 188
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    24 non-null     object 
#   1   geonombreFundar    24 non-null     object 
#   2   anio               24 non-null     int64  
#   3   salario_relativo   24 non-null     float64
#   4   tasa_empleo_mujer  24 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio |   salario_relativo |   tasa_empleo_mujer |
#  |----:|:------------------|:------------------|-------:|-------------------:|--------------------:|
#  | 165 | AR-B              | Buenos Aires      |   2023 |            100.367 |            0.625285 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'anio'], axis=1)
#  Index: 24 entries, 165 to 188
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    24 non-null     object 
#   1   salario_relativo   24 non-null     float64
#   2   tasa_empleo_mujer  24 non-null     float64
#  
#  |     | geonombreFundar   |   salario_relativo |   tasa_empleo_mujer |
#  |----:|:------------------|-------------------:|--------------------:|
#  | 165 | Buenos Aires      |            100.367 |             62.5285 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_empleo_mujer', k=100)
#  Index: 24 entries, 165 to 188
#  Data columns (total 3 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    24 non-null     object 
#   1   salario_relativo   24 non-null     float64
#   2   tasa_empleo_mujer  24 non-null     float64
#  
#  |     | geonombreFundar   |   salario_relativo |   tasa_empleo_mujer |
#  |----:|:------------------|-------------------:|--------------------:|
#  | 165 | Buenos Aires      |            100.367 |             62.5285 |
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
#  |    | geonombreFundar   | variable         |   valor |
#  |---:|:------------------|:-----------------|--------:|
#  |  0 | Buenos Aires      | salario_relativo | 100.367 |
#  
#  ------------------------------
#  