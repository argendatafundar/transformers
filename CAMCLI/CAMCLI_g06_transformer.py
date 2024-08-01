from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
	rename_cols(map={'sector': 'nivel1', 'subsector': 'nivel2', 'valor_en_porcent': 'valor'}),
	drop_col(col=['valor_en_mtco2e'], axis=1),
	replace_value(col='nivel1', curr_value='Procesos industriales y uso de productos', new_value='PIUP')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   sector            15 non-null     object 
#   1   anio              15 non-null     int64  
#   2   subsector         15 non-null     object 
#   3   valor_en_mtco2e   15 non-null     float64
#   4   valor_en_porcent  15 non-null     float64
#  
#  |    | sector   |   anio | subsector                        |   valor_en_mtco2e |   valor_en_porcent |
#  |---:|:---------|-------:|:---------------------------------|------------------:|-------------------:|
#  |  0 | AGSyOUT  |   2018 | Emisiones de la quema de biomasa |           4.52477 |            1.23665 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 15 entries, 0 to 14
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   sector            15 non-null     object 
#   1   subsector         15 non-null     object 
#   2   valor_en_mtco2e   15 non-null     float64
#   3   valor_en_porcent  15 non-null     float64
#  
#  |    | sector   | subsector                        |   valor_en_mtco2e |   valor_en_porcent |
#  |---:|:---------|:---------------------------------|------------------:|-------------------:|
#  |  0 | AGSyOUT  | Emisiones de la quema de biomasa |           4.52477 |            1.23665 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'nivel1', 'subsector': 'nivel2', 'valor_en_porcent': 'valor'})
#  Index: 15 entries, 0 to 14
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   nivel1           15 non-null     object 
#   1   nivel2           15 non-null     object 
#   2   valor_en_mtco2e  15 non-null     float64
#   3   valor            15 non-null     float64
#  
#  |    | nivel1   | nivel2                           |   valor_en_mtco2e |   valor |
#  |---:|:---------|:---------------------------------|------------------:|--------:|
#  |  0 | AGSyOUT  | Emisiones de la quema de biomasa |           4.52477 | 1.23665 |
#  
#  ------------------------------
#  
#  drop_col(col=['valor_en_mtco2e'], axis=1)
#  Index: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  15 non-null     object 
#   1   nivel2  15 non-null     object 
#   2   valor   15 non-null     float64
#  
#  |    | nivel1   | nivel2                           |   valor |
#  |---:|:---------|:---------------------------------|--------:|
#  |  0 | AGSyOUT  | Emisiones de la quema de biomasa | 1.23665 |
#  
#  ------------------------------
#  
#  replace_value(col='nivel1', curr_value='Procesos industriales y uso de productos', new_value='PIUP')
#  Index: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  15 non-null     object 
#   1   nivel2  15 non-null     object 
#   2   valor   15 non-null     float64
#  
#  |    | nivel1   | nivel2                           |   valor |
#  |---:|:---------|:---------------------------------|--------:|
#  |  0 | AGSyOUT  | Emisiones de la quema de biomasa | 1.23665 |
#  
#  ------------------------------
#  