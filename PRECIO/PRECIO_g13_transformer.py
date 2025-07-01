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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='porcentaje', k=100),
	query(condition="rubro != 'Total gasto de consumo'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 39 entries, 0 to 38
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   rubro       39 non-null     object 
#   1   periodo     39 non-null     object 
#   2   porcentaje  39 non-null     float64
#  
#  |    | rubro                              | periodo   |   porcentaje |
#  |---:|:-----------------------------------|:----------|-------------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997 |        0.288 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='porcentaje', k=100)
#  RangeIndex: 39 entries, 0 to 38
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   rubro       39 non-null     object 
#   1   periodo     39 non-null     object 
#   2   porcentaje  39 non-null     float64
#  
#  |    | rubro                              | periodo   |   porcentaje |
#  |---:|:-----------------------------------|:----------|-------------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997 |         28.8 |
#  
#  ------------------------------
#  
#  query(condition="rubro != 'Total gasto de consumo'")
#  Index: 36 entries, 0 to 38
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   rubro       36 non-null     object 
#   1   periodo     36 non-null     object 
#   2   porcentaje  36 non-null     float64
#  
#  |    | rubro                              | periodo   |   porcentaje |
#  |---:|:-----------------------------------|:----------|-------------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997 |         28.8 |
#  
#  ------------------------------
#  