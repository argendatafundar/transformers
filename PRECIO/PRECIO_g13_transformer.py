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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'periodo': 'indicador'}),
	rename_cols(map={'rubro': 'indicador'}),
	rename_cols(map={'porcentaje': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   rubro       36 non-null     object 
#   1   periodo     36 non-null     object 
#   2   porcentaje  36 non-null     float64
#  
#  |    | rubro                              | periodo   |   porcentaje |
#  |---:|:-----------------------------------|:----------|-------------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997 |        0.288 |
#  
#  ------------------------------
#  
#  rename_cols(map={'periodo': 'indicador'})
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   rubro       36 non-null     object 
#   1   indicador   36 non-null     object 
#   2   porcentaje  36 non-null     float64
#  
#  |    | rubro                              | indicador   |   porcentaje |
#  |---:|:-----------------------------------|:------------|-------------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997   |        0.288 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rubro': 'indicador'})
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   indicador   36 non-null     object 
#   1   indicador   36 non-null     object 
#   2   porcentaje  36 non-null     float64
#  
#  |    | indicador                          | indicador   |   porcentaje |
#  |---:|:-----------------------------------|:------------|-------------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997   |        0.288 |
#  
#  ------------------------------
#  
#  rename_cols(map={'porcentaje': 'valor'})
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  36 non-null     object 
#   1   indicador  36 non-null     object 
#   2   valor      36 non-null     float64
#  
#  |    | indicador                          | indicador   |   valor |
#  |---:|:-----------------------------------|:------------|--------:|
#  |  0 | Alimentos y bebidas no alcohólicas | 1996-1997   |   0.288 |
#  
#  ------------------------------
#  