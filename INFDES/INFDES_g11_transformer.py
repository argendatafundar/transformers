from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition="iso3 == 'ARG'"),
	drop_col(col='iso3', axis=1),
	drop_col(col='pais_desc', axis=1),
	rename_cols(map={'tasa_desempleo': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            66 non-null     object 
#   1   anio            66 non-null     int64  
#   2   tasa_desempleo  66 non-null     float64
#   3   pais_desc       66 non-null     object 
#  
#  |    | iso3   |   anio |   tasa_desempleo | pais_desc   |
#  |---:|:-------|-------:|-----------------:|:------------|
#  |  0 | WLD    |   2023 |        0.0512316 | Mundo       |
#  
#  ------------------------------
#  
#  query(condition="iso3 == 'ARG'")
#  Index: 33 entries, 33 to 65
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            33 non-null     object 
#   1   anio            33 non-null     int64  
#   2   tasa_desempleo  33 non-null     float64
#   3   pais_desc       33 non-null     object 
#  
#  |    | iso3   |   anio |   tasa_desempleo | pais_desc   |
#  |---:|:-------|-------:|-----------------:|:------------|
#  | 33 | ARG    |   2023 |          0.06841 | Argentina   |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 33 entries, 33 to 65
#  Data columns (total 3 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            33 non-null     int64  
#   1   tasa_desempleo  33 non-null     float64
#   2   pais_desc       33 non-null     object 
#  
#  |    |   anio |   tasa_desempleo | pais_desc   |
#  |---:|-------:|-----------------:|:------------|
#  | 33 |   2023 |          0.06841 | Argentina   |
#  
#  ------------------------------
#  
#  drop_col(col='pais_desc', axis=1)
#  Index: 33 entries, 33 to 65
#  Data columns (total 2 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            33 non-null     int64  
#   1   tasa_desempleo  33 non-null     float64
#  
#  |    |   anio |   tasa_desempleo |
#  |---:|-------:|-----------------:|
#  | 33 |   2023 |          0.06841 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tasa_desempleo': 'valor'})
#  Index: 33 entries, 33 to 65
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    33 non-null     int64  
#   1   valor   33 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  | 33 |   2023 |   6.841 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 33 entries, 33 to 65
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    33 non-null     int64  
#   1   valor   33 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  | 33 |   2023 |   6.841 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    33 non-null     int64  
#   1   valor   33 non-null     float64
#  
#  |    |   anio |   valor |
#  |---:|-------:|--------:|
#  |  0 |   1991 |    5.44 |
#  
#  ------------------------------
#  