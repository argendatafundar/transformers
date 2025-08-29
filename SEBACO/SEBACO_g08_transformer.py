from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='sector', replacements={'Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones)': 'Otros servicios', 'Servicios de inversión y desarrollo': 'Servicios de I+D', 'Software y servicios informáticos': 'SSI'}),
	multiplicar_por_escalar(col='prop', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   sector  24 non-null     object 
#   1   anio    24 non-null     int64  
#   2   prop    24 non-null     float64
#  
#  |    | sector                                                                                      |   anio |      prop |
#  |---:|:--------------------------------------------------------------------------------------------|-------:|----------:|
#  |  0 | Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones) |   2015 | 0.0279136 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='sector', replacements={'Otros servicios (empresariales, relacionados con la salud humana y animal y comunicaciones)': 'Otros servicios', 'Servicios de inversión y desarrollo': 'Servicios de I+D', 'Software y servicios informáticos': 'SSI'})
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   sector  24 non-null     object 
#   1   anio    24 non-null     int64  
#   2   prop    24 non-null     float64
#  
#  |    | sector          |   anio |    prop |
#  |---:|:----------------|-------:|--------:|
#  |  0 | Otros servicios |   2015 | 2.79136 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop', k=100)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   sector  24 non-null     object 
#   1   anio    24 non-null     int64  
#   2   prop    24 non-null     float64
#  
#  |    | sector          |   anio |    prop |
#  |---:|:----------------|-------:|--------:|
#  |  0 | Otros servicios |   2015 | 2.79136 |
#  
#  ------------------------------
#  