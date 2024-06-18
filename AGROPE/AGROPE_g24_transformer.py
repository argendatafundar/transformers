from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'year': 'anio', 'share_expo': 'valor'}),
	drop_col(col='pais', axis=1),
	drop_col(col='product', axis=1),
	drop_col(col='iso3', axis=1),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        60 non-null     int64  
#   1   iso3        60 non-null     object 
#   2   pais        60 non-null     object 
#   3   share_expo  60 non-null     float64
#   4   product     60 non-null     object 
#  
#  |    |   year | iso3   | pais      |   share_expo | product                                    |
#  |---:|-------:|:-------|:----------|-------------:|:-------------------------------------------|
#  |  0 |   1962 | ARG    | Argentina |     0.110198 | Carne bovina, fresca, enfriada o congelada |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'share_expo': 'valor'})
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 5 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   anio     60 non-null     int64  
#   1   iso3     60 non-null     object 
#   2   pais     60 non-null     object 
#   3   valor    60 non-null     float64
#   4   product  60 non-null     object 
#  
#  |    |   anio | iso3   | pais      |    valor | product                                    |
#  |---:|-------:|:-------|:----------|---------:|:-------------------------------------------|
#  |  0 |   1962 | ARG    | Argentina | 0.110198 | Carne bovina, fresca, enfriada o congelada |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 4 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   anio     60 non-null     int64  
#   1   iso3     60 non-null     object 
#   2   valor    60 non-null     float64
#   3   product  60 non-null     object 
#  
#  |    |   anio | iso3   |    valor | product                                    |
#  |---:|-------:|:-------|---------:|:-------------------------------------------|
#  |  0 |   1962 | ARG    | 0.110198 | Carne bovina, fresca, enfriada o congelada |
#  
#  ------------------------------
#  
#  drop_col(col='product', axis=1)
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    60 non-null     int64  
#   1   iso3    60 non-null     object 
#   2   valor   60 non-null     float64
#  
#  |    |   anio | iso3   |    valor |
#  |---:|-------:|:-------|---------:|
#  |  0 |   1962 | ARG    | 0.110198 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    60 non-null     int64  
#   1   valor   60 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1962 | 11.0198 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    60 non-null     int64  
#   1   valor   60 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1962 | 11.0198 |
#  
#  ------------------------------
#  