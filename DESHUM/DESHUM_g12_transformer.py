from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def fill_mundo_world(df: DataFrame) -> DataFrame:
    df.loc[
        (df['country'] == 'World') & df['geonombreFundar'].isna(),
        'geonombreFundar'
    ] = 'Mundo'
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	rename_cols(map={'sexo': 'indicador', 'idh': 'valor'}),
	fill_mundo_world(),
	query(condition="geonombreFundar in ('Argentina', 'Yemen','Noruega','Brasil','Suiza','Mundo','Burundi')")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 13596 entries, 0 to 13595
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  13530 non-null  object 
#   1   geonombreFundar  13530 non-null  object 
#   2   country          13596 non-null  object 
#   3   anio             13596 non-null  int64  
#   4   sexo             13596 non-null  object 
#   5   idh              10028 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | country     |   anio | sexo    |   idh |
#  |---:|:------------------|:------------------|:------------|-------:|:--------|------:|
#  |  0 | AFG               | Afganistán        | Afghanistan |   1990 | Varones |   nan |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 412 entries, 32 to 13595
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  410 non-null    object 
#   1   geonombreFundar  410 non-null    object 
#   2   country          412 non-null    object 
#   3   anio             412 non-null    int64  
#   4   sexo             412 non-null    object 
#   5   idh              386 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | country     |   anio | sexo    |      idh |
#  |---:|:------------------|:------------------|:------------|-------:|:--------|---------:|
#  | 32 | AFG               | Afganistán        | Afghanistan |   2022 | Varones | 0.534145 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sexo': 'indicador', 'idh': 'valor'})
#  Index: 412 entries, 32 to 13595
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  410 non-null    object 
#   1   geonombreFundar  412 non-null    object 
#   2   country          412 non-null    object 
#   3   anio             412 non-null    int64  
#   4   indicador        412 non-null    object 
#   5   valor            386 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | country     |   anio | indicador   |    valor |
#  |---:|:------------------|:------------------|:------------|-------:|:------------|---------:|
#  | 32 | AFG               | Afganistán        | Afghanistan |   2022 | Varones     | 0.534145 |
#  
#  ------------------------------
#  
#  fill_mundo_world()
#  Index: 412 entries, 32 to 13595
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  410 non-null    object 
#   1   geonombreFundar  412 non-null    object 
#   2   country          412 non-null    object 
#   3   anio             412 non-null    int64  
#   4   indicador        412 non-null    object 
#   5   valor            386 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | country     |   anio | indicador   |    valor |
#  |---:|:------------------|:------------------|:------------|-------:|:------------|---------:|
#  | 32 | AFG               | Afganistán        | Afghanistan |   2022 | Varones     | 0.534145 |
#  
#  ------------------------------
#  
#  query(condition="geonombreFundar in ('Argentina', 'Yemen','Noruega','Brasil','Suiza','Mundo','Burundi')")
#  Index: 14 entries, 197 to 13595
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  12 non-null     object 
#   1   geonombreFundar  14 non-null     object 
#   2   country          14 non-null     object 
#   3   anio             14 non-null     int64  
#   4   indicador        14 non-null     object 
#   5   valor            14 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   | country   |   anio | indicador   |    valor |
#  |----:|:------------------|:------------------|:----------|-------:|:------------|---------:|
#  | 197 | ARG               | Argentina         | Argentina |   2022 | Varones     | 0.845473 |
#  
#  ------------------------------
#  