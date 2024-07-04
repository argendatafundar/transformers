from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='iso3', axis=1),
	replace_value(col='pais_nombre', curr_value='Resto LATAM', new_value='Resto de América Latina'),
	rename_cols(map={'pais_nombre': 'categoria', 'participacion': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   iso3           48 non-null     object 
#   1   pais_nombre    64 non-null     object 
#   2   anio           64 non-null     int64  
#   3   participacion  64 non-null     float64
#  
#  |    |   iso3 | pais_nombre   |   anio |   participacion |
#  |---:|-------:|:--------------|-------:|----------------:|
#  |  0 |    nan | Resto LATAM   |   1820 |        0.405228 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   pais_nombre    64 non-null     object 
#   1   anio           64 non-null     int64  
#   2   participacion  64 non-null     float64
#  
#  |    | pais_nombre   |   anio |   participacion |
#  |---:|:--------------|-------:|----------------:|
#  |  0 | Resto LATAM   |   1820 |        0.405228 |
#  
#  ------------------------------
#  
#  replace_value(col='pais_nombre', curr_value='Resto LATAM', new_value='Resto de América Latina')
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   pais_nombre    64 non-null     object 
#   1   anio           64 non-null     int64  
#   2   participacion  64 non-null     float64
#  
#  |    | pais_nombre             |   anio |   participacion |
#  |---:|:------------------------|-------:|----------------:|
#  |  0 | Resto de América Latina |   1820 |        0.405228 |
#  
#  ------------------------------
#  
#  rename_cols(map={'pais_nombre': 'categoria', 'participacion': 'valor'})
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  64 non-null     object 
#   1   anio       64 non-null     int64  
#   2   valor      64 non-null     float64
#  
#  |    | categoria               |   anio |   valor |
#  |---:|:------------------------|-------:|--------:|
#  |  0 | Resto de América Latina |   1820 | 40.5228 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  64 non-null     object 
#   1   anio       64 non-null     int64  
#   2   valor      64 non-null     float64
#  
#  |    | categoria               |   anio |   valor |
#  |---:|:------------------------|-------:|--------:|
#  |  0 | Resto de América Latina |   1820 | 40.5228 |
#  
#  ------------------------------
#  