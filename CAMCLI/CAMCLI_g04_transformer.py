from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'sector': 'nivel1', 'subsector': 'nivel2', 'subsubsector': 'nivel3', 'valor_en_porcent': 'valor'}),
	replace_value(col='nivel1', curr_value='Procesos industriales', new_value='PIUP'),
	replace_value(col='nivel1', curr_value='Agricultura', new_value='AGSyOUT')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 19 entries, 0 to 18
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   sector            19 non-null     object 
#   1   subsector         19 non-null     object 
#   2   subsubsector      19 non-null     object 
#   3   anio              19 non-null     int64  
#   4   valor_en_porcent  19 non-null     float64
#  
#  |    | sector      | subsector                        | subsubsector                     |   anio |   valor_en_porcent |
#  |---:|:------------|:---------------------------------|:---------------------------------|-------:|-------------------:|
#  |  0 | Agricultura | Quema de biomasa, suelos y arroz | Quema de biomasa, suelos y arroz |   2024 |              4.924 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'nivel1', 'subsector': 'nivel2', 'subsubsector': 'nivel3', 'valor_en_porcent': 'valor'})
#  RangeIndex: 19 entries, 0 to 18
#  Data columns (total 5 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  19 non-null     object 
#   1   nivel2  19 non-null     object 
#   2   nivel3  19 non-null     object 
#   3   anio    19 non-null     int64  
#   4   valor   19 non-null     float64
#  
#  |    | nivel1      | nivel2                           | nivel3                           |   anio |   valor |
#  |---:|:------------|:---------------------------------|:---------------------------------|-------:|--------:|
#  |  0 | Agricultura | Quema de biomasa, suelos y arroz | Quema de biomasa, suelos y arroz |   2024 |   4.924 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel1', curr_value='Procesos industriales', new_value='PIUP')
#  RangeIndex: 19 entries, 0 to 18
#  Data columns (total 5 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  19 non-null     object 
#   1   nivel2  19 non-null     object 
#   2   nivel3  19 non-null     object 
#   3   anio    19 non-null     int64  
#   4   valor   19 non-null     float64
#  
#  |    | nivel1      | nivel2                           | nivel3                           |   anio |   valor |
#  |---:|:------------|:---------------------------------|:---------------------------------|-------:|--------:|
#  |  0 | Agricultura | Quema de biomasa, suelos y arroz | Quema de biomasa, suelos y arroz |   2024 |   4.924 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel1', curr_value='Agricultura', new_value='AGSyOUT')
#  RangeIndex: 19 entries, 0 to 18
#  Data columns (total 5 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  19 non-null     object 
#   1   nivel2  19 non-null     object 
#   2   nivel3  19 non-null     object 
#   3   anio    19 non-null     int64  
#   4   valor   19 non-null     float64
#  
#  |    | nivel1   | nivel2                           | nivel3                           |   anio |   valor |
#  |---:|:---------|:---------------------------------|:---------------------------------|-------:|--------:|
#  |  0 | AGSyOUT  | Quema de biomasa, suelos y arroz | Quema de biomasa, suelos y arroz |   2024 |   4.924 |
#  
#  ------------------------------
#  