from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'periodo': 'indicador'}),
	rename_cols(map={'rubro': 'indicador'}),
	rename_cols(map={'porcentaje': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100)
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
#  rename_cols(map={'periodo': 'indicador'})
#  RangeIndex: 39 entries, 0 to 38
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   rubro       39 non-null     object 
#   1   indicador   39 non-null     object 
#   2   porcentaje  39 non-null     float64
#  
#  |    | rubro                              | indicador   |   porcentaje |
#  |---:|:-----------------------------------|:------------|-------------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997   |        0.288 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rubro': 'indicador'})
#  RangeIndex: 39 entries, 0 to 38
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   indicador   39 non-null     object 
#   1   indicador   39 non-null     object 
#   2   porcentaje  39 non-null     float64
#  
#  |    | indicador                          | indicador   |   porcentaje |
#  |---:|:-----------------------------------|:------------|-------------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997   |        0.288 |
#  
#  ------------------------------
#  
#  rename_cols(map={'porcentaje': 'valor'})
#  RangeIndex: 39 entries, 0 to 38
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  39 non-null     object 
#   1   indicador  39 non-null     object 
#   2   valor      39 non-null     float64
#  
#  |    | indicador                          | indicador   |   valor |
#  |---:|:-----------------------------------|:------------|--------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997   |    28.8 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 39 entries, 0 to 38
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  39 non-null     object 
#   1   indicador  39 non-null     object 
#   2   valor      39 non-null     float64
#  
#  |    | indicador                          | indicador   |   valor |
#  |---:|:-----------------------------------|:------------|--------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997   |    28.8 |
#  
#  ------------------------------
#  