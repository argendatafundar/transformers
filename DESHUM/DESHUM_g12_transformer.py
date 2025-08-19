from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_categorica(df, col1:str, order1:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    return df.sort_values(by=[col1])

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	rename_cols(map={'sexo': 'indicador', 'idh': 'valor'}),
	query(condition="geonombreFundar in ('Argentina', 'Yemen','Noruega','Brasil','Suiza','Mundo','Burundi')"),
	ordenar_categorica(col1='geonombreFundar', order1=['Argentina', 'Brasil', 'Burundi', 'Noruega', 'Suiza', 'Yemen', 'Mundo'])
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
#   1   geonombreFundar  410 non-null    object 
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
#  Index: 12 entries, 197 to 13133
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  12 non-null     object  
#   1   geonombreFundar  12 non-null     category
#   2   country          12 non-null     object  
#   3   anio             12 non-null     int64   
#   4   indicador        12 non-null     object  
#   5   valor            12 non-null     float64 
#  
#  |     | geocodigoFundar   | geonombreFundar   | country   |   anio | indicador   |    valor |
#  |----:|:------------------|:------------------|:----------|-------:|:------------|---------:|
#  | 197 | ARG               | Argentina         | Argentina |   2022 | Varones     | 0.845473 |
#  
#  ------------------------------
#  
#  ordenar_categorica(col1='geonombreFundar', order1=['Argentina', 'Brasil', 'Burundi', 'Noruega', 'Suiza', 'Yemen', 'Mundo'])
#  Index: 12 entries, 197 to 13133
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  12 non-null     object  
#   1   geonombreFundar  12 non-null     category
#   2   country          12 non-null     object  
#   3   anio             12 non-null     int64   
#   4   indicador        12 non-null     object  
#   5   valor            12 non-null     float64 
#  
#  |     | geocodigoFundar   | geonombreFundar   | country   |   anio | indicador   |    valor |
#  |----:|:------------------|:------------------|:----------|-------:|:------------|---------:|
#  | 197 | ARG               | Argentina         | Argentina |   2022 | Varones     | 0.845473 |
#  
#  ------------------------------
#  