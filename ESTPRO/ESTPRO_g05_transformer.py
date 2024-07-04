from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

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
query(condition='anio == anio.max() & anio >= 2023 | anio == 2019'),
	replace_value(col='iso3', curr_value='A5_A7', new_value='ZSCA'),
	replace_value(col='iso3', curr_value='E', new_value='ZEUR'),
	replace_value(col='iso3', curr_value='EU28XEU15', new_value='EU13'),
	replace_value(col='iso3', curr_value='F', new_value='X06'),
	replace_value(col='iso3', curr_value='NAFTA', new_value='EU15'),
	replace_value(col='iso3', curr_value='S2', new_value='EASIA'),
	replace_value(col='iso3', curr_value='S2_S8', new_value='ZASI'),
	replace_value(col='iso3', curr_value='W', new_value='WLD'),
	replace_value(col='iso3', curr_value='WXD', new_value='ROW'),
	replace_value(col='iso3', curr_value='WXOECD', new_value='NONOECD'),
	replace_value(col='iso3', curr_value='W_O', new_value='ZOTH'),
	rename_cols(map={'particip_bys_alta_intensidad': 'valor', 'iso3': 'geocodigo'}),
	drop_col(col=['iso3_desc_fundar', 'es_agregacion'], axis=1),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2470 entries, 0 to 2469
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          2470 non-null   int64  
#   1   iso3                          2470 non-null   object 
#   2   iso3_desc_fundar              2470 non-null   object 
#   3   es_agregacion                 2470 non-null   int64  
#   4   particip_bys_alta_intensidad  2470 non-null   float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  |  0 |   1995 | A5_A7  | América del Sur y Central |               1 |                       0.138143 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max() & anio >= 2023 | anio == 2019')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | A5_A7  | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='A5_A7', new_value='ZSCA')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | ZSCA   | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='E', new_value='ZEUR')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | ZSCA   | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='EU28XEU15', new_value='EU13')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | ZSCA   | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F', new_value='X06')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | ZSCA   | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='NAFTA', new_value='EU15')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | ZSCA   | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='S2', new_value='EASIA')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | ZSCA   | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='S2_S8', new_value='ZASI')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | ZSCA   | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='W', new_value='WLD')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | ZSCA   | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='WXD', new_value='ROW')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | ZSCA   | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='WXOECD', new_value='NONOECD')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | ZSCA   | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='W_O', new_value='ZOTH')
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   anio                          95 non-null     int64  
#   1   iso3                          95 non-null     object 
#   2   iso3_desc_fundar              95 non-null     object 
#   3   es_agregacion                 95 non-null     int64  
#   4   particip_bys_alta_intensidad  95 non-null     float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar          |   es_agregacion |   particip_bys_alta_intensidad |
#  |---:|-------:|:-------|:--------------------------|----------------:|-------------------------------:|
#  | 24 |   2019 | ZSCA   | América del Sur y Central |               1 |                       0.121979 |
#  
#  ------------------------------
#  
#  rename_cols(map={'particip_bys_alta_intensidad': 'valor', 'iso3': 'geocodigo'})
#  Index: 95 entries, 24 to 2468
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              95 non-null     int64  
#   1   geocodigo         95 non-null     object 
#   2   iso3_desc_fundar  95 non-null     object 
#   3   es_agregacion     95 non-null     int64  
#   4   valor             95 non-null     float64
#  
#  |    |   anio | geocodigo   | iso3_desc_fundar          |   es_agregacion |    valor |
#  |---:|-------:|:------------|:--------------------------|----------------:|---------:|
#  | 24 |   2019 | ZSCA        | América del Sur y Central |               1 | 0.121979 |
#  
#  ------------------------------
#  
#  drop_col(col=['iso3_desc_fundar', 'es_agregacion'], axis=1)
#  Index: 95 entries, 24 to 2468
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       95 non-null     int64  
#   1   geocodigo  95 non-null     object 
#   2   valor      95 non-null     float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  | 24 |   2019 | ZSCA        | 12.1979 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 95 entries, 24 to 2468
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       95 non-null     int64  
#   1   geocodigo  95 non-null     object 
#   2   valor      95 non-null     float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  | 24 |   2019 | ZSCA        | 12.1979 |
#  
#  ------------------------------
#  