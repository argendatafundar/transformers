from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

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
	multiplicar_por_escalar(col='prop', k=100),
	ordenar_dos_columnas(col1='sector', order1=['Servicios de I+D', 'SSI', 'Otros servicios'], col2='anio', order2=[2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022])
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
#   0   sector  24 non-null     category
#   1   anio    24 non-null     category
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
#   0   sector  24 non-null     category
#   1   anio    24 non-null     category
#   2   prop    24 non-null     float64 
#  
#  |    | sector          |   anio |    prop |
#  |---:|:----------------|-------:|--------:|
#  |  0 | Otros servicios |   2015 | 2.79136 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='sector', order1=['Servicios de I+D', 'SSI', 'Otros servicios'], col2='anio', order2=[2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022])
#  Index: 24 entries, 8 to 7
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype   
#  ---  ------  --------------  -----   
#   0   sector  24 non-null     category
#   1   anio    24 non-null     category
#   2   prop    24 non-null     float64 
#  
#  |    | sector           |   anio |    prop |
#  |---:|:-----------------|-------:|--------:|
#  |  8 | Servicios de I+D |   2015 | 21.4179 |
#  
#  ------------------------------
#  