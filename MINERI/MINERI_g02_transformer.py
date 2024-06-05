from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys=['codigo_pais', 'nombre_pais', 'mineral'], value_name='valor', var_name='indicador'),
	drop_col(col='nombre_pais', axis=1),
	rename_cols(map={'codigo_pais': 'geoselector'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 13 entries, 0 to 12
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   codigo_pais      13 non-null     object 
#   1   nombre_pais      13 non-null     object 
#   2   share            13 non-null     float64
#   3   mineral          13 non-null     object 
#   4   tamanio_mercado  13 non-null     float64
#  
#  |    | codigo_pais   | nombre_pais   |   share | mineral   |   tamanio_mercado |
#  |---:|:--------------|:--------------|--------:|:----------|------------------:|
#  |  0 | CHN           | China         |    10.6 | Oro       |       1.73705e+08 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['codigo_pais', 'nombre_pais', 'mineral'], value_name='valor', var_name='indicador')
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   codigo_pais  26 non-null     object 
#   1   nombre_pais  26 non-null     object 
#   2   mineral      26 non-null     object 
#   3   indicador    26 non-null     object 
#   4   valor        26 non-null     float64
#  
#  |    | codigo_pais   | nombre_pais   | mineral   | indicador   |   valor |
#  |---:|:--------------|:--------------|:----------|:------------|--------:|
#  |  0 | CHN           | China         | Oro       | share       |    10.6 |
#  
#  ------------------------------
#  
#  drop_col(col='nombre_pais', axis=1)
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   codigo_pais  26 non-null     object 
#   1   mineral      26 non-null     object 
#   2   indicador    26 non-null     object 
#   3   valor        26 non-null     float64
#  
#  |    | codigo_pais   | mineral   | indicador   |   valor |
#  |---:|:--------------|:----------|:------------|--------:|
#  |  0 | CHN           | Oro       | share       |    10.6 |
#  
#  ------------------------------
#  
#  rename_cols(map={'codigo_pais': 'geoselector'})
#  RangeIndex: 26 entries, 0 to 25
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  26 non-null     object 
#   1   mineral      26 non-null     object 
#   2   indicador    26 non-null     object 
#   3   valor        26 non-null     float64
#  
#  |    | geoselector   | mineral   | indicador   |   valor |
#  |---:|:--------------|:----------|:------------|--------:|
#  |  0 | CHN           | Oro       | share       |    10.6 |
#  
#  ------------------------------
#  