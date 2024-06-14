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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='anio == anio.max()'),
	drop_col(col='anio', axis=1),
	drop_col(col='provincia_id', axis=1),
	drop_col(col='vab_pb', axis=1),
	rename_cols(map={'provincia_nombre': 'subindicador', 'region': 'indicador', 'participacion': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 456 entries, 0 to 455
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   provincia_id      456 non-null    int64  
#   1   provincia_nombre  456 non-null    object 
#   2   region            456 non-null    object 
#   3   anio              456 non-null    int64  
#   4   vab_pb            456 non-null    float64
#   5   participacion     456 non-null    float64
#  
#  |    |   provincia_id | provincia_nombre                | region          |   anio |   vab_pb |   participacion |
#  |---:|---------------:|:--------------------------------|:----------------|-------:|---------:|----------------:|
#  |  0 |              2 | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |   2004 |  85706.9 |           0.208 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 24 entries, 432 to 455
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   provincia_id      24 non-null     int64  
#   1   provincia_nombre  24 non-null     object 
#   2   region            24 non-null     object 
#   3   anio              24 non-null     int64  
#   4   vab_pb            24 non-null     float64
#   5   participacion     24 non-null     float64
#  
#  |     |   provincia_id | provincia_nombre                | region          |   anio |   vab_pb |   participacion |
#  |----:|---------------:|:--------------------------------|:----------------|-------:|---------:|----------------:|
#  | 432 |              2 | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |   2022 |   119160 |          0.1973 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 24 entries, 432 to 455
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   provincia_id      24 non-null     int64  
#   1   provincia_nombre  24 non-null     object 
#   2   region            24 non-null     object 
#   3   vab_pb            24 non-null     float64
#   4   participacion     24 non-null     float64
#  
#  |     |   provincia_id | provincia_nombre                | region          |   vab_pb |   participacion |
#  |----:|---------------:|:--------------------------------|:----------------|---------:|----------------:|
#  | 432 |              2 | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |   119160 |          0.1973 |
#  
#  ------------------------------
#  
#  drop_col(col='provincia_id', axis=1)
#  Index: 24 entries, 432 to 455
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   provincia_nombre  24 non-null     object 
#   1   region            24 non-null     object 
#   2   vab_pb            24 non-null     float64
#   3   participacion     24 non-null     float64
#  
#  |     | provincia_nombre                | region          |   vab_pb |   participacion |
#  |----:|:--------------------------------|:----------------|---------:|----------------:|
#  | 432 | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |   119160 |          0.1973 |
#  
#  ------------------------------
#  
#  drop_col(col='vab_pb', axis=1)
#  Index: 24 entries, 432 to 455
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   provincia_nombre  24 non-null     object 
#   1   region            24 non-null     object 
#   2   participacion     24 non-null     float64
#  
#  |     | provincia_nombre                | region          |   participacion |
#  |----:|:--------------------------------|:----------------|----------------:|
#  | 432 | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |          0.1973 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia_nombre': 'subindicador', 'region': 'indicador', 'participacion': 'valor'})
#  Index: 24 entries, 432 to 455
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   subindicador  24 non-null     object 
#   1   indicador     24 non-null     object 
#   2   valor         24 non-null     float64
#  
#  |     | subindicador                    | indicador       |   valor |
#  |----:|:--------------------------------|:----------------|--------:|
#  | 432 | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |   19.73 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  Index: 24 entries, 432 to 455
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   subindicador  24 non-null     object 
#   1   indicador     24 non-null     object 
#   2   valor         24 non-null     float64
#  
#  |     | subindicador                    | indicador       |   valor |
#  |----:|:--------------------------------|:----------------|--------:|
#  | 432 | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |   19.73 |
#  
#  ------------------------------
#  