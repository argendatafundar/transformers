from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def long_to_wide(df:DataFrame, index:list[str], columns:str, values:str):
    df = df.pivot(index=index, columns=columns, values=values).reset_index()
    df.index.name = None
    df.columns.name = None
    df.columns = [str(col) for col in df.columns]  # Convertir columnas a str
    return df  

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio in [2009, 2024]'),
	long_to_wide(index=['geocodigoFundar', 'geonombreFundar', 'idp', 'institucion'], columns='anio', values='ranking'),
	drop_na(col='2009'),
	drop_na(col='2024'),
	pivot_longer(id_cols=['geocodigoFundar', 'geonombreFundar', 'idp', 'institucion'], names_to_col='anio', values_to_col='ranking')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 449 entries, 0 to 448
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  449 non-null    object
#   1   geonombreFundar  449 non-null    object
#   2   anio             449 non-null    int64 
#   3   idp              449 non-null    int64 
#   4   institucion      449 non-null    object
#   5   ranking          449 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   idp | institucion                                                |   ranking |
#  |---:|:------------------|:------------------|-------:|------:|:-----------------------------------------------------------|----------:|
#  |  0 | ARG               | Argentina         |   2009 | 25417 | Consejo Nacional de Investigaciones Cientificas y Tecnicas |         1 |
#  
#  ------------------------------
#  
#  query(condition='anio in [2009, 2024]')
#  Index: 67 entries, 0 to 448
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  67 non-null     object
#   1   geonombreFundar  67 non-null     object
#   2   anio             67 non-null     int64 
#   3   idp              67 non-null     int64 
#   4   institucion      67 non-null     object
#   5   ranking          67 non-null     int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   idp | institucion                                                |   ranking |
#  |---:|:------------------|:------------------|-------:|------:|:-----------------------------------------------------------|----------:|
#  |  0 | ARG               | Argentina         |   2009 | 25417 | Consejo Nacional de Investigaciones Cientificas y Tecnicas |         1 |
#  
#  ------------------------------
#  
#  long_to_wide(index=['geocodigoFundar', 'geonombreFundar', 'idp', 'institucion'], columns='anio', values='ranking')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  48 non-null     object 
#   1   geonombreFundar  48 non-null     object 
#   2   idp              48 non-null     int64  
#   3   institucion      48 non-null     object 
#   4   2009             19 non-null     float64
#   5   2024             48 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   idp | institucion              |   2009 |   2024 |
#  |---:|:------------------|:------------------|------:|:-------------------------|-------:|-------:|
#  |  0 | ARG               | Argentina         | 25377 | Centro Atomico Bariloche |    nan |     39 |
#  
#  ------------------------------
#  
#  drop_na(col='2009')
#  Index: 19 entries, 1 to 44
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  19 non-null     object 
#   1   geonombreFundar  19 non-null     object 
#   2   idp              19 non-null     int64  
#   3   institucion      19 non-null     object 
#   4   2009             19 non-null     float64
#   5   2024             19 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   idp | institucion                                |   2009 |   2024 |
#  |---:|:------------------|:------------------|------:|:-------------------------------------------|-------:|-------:|
#  |  1 | ARG               | Argentina         | 25408 | Centro Cientifico Tecnologico Bahia Blanca |      8 |     21 |
#  
#  ------------------------------
#  
#  drop_na(col='2024')
#  Index: 19 entries, 1 to 44
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  19 non-null     object 
#   1   geonombreFundar  19 non-null     object 
#   2   idp              19 non-null     int64  
#   3   institucion      19 non-null     object 
#   4   2009             19 non-null     float64
#   5   2024             19 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   idp | institucion                                |   2009 |   2024 |
#  |---:|:------------------|:------------------|------:|:-------------------------------------------|-------:|-------:|
#  |  1 | ARG               | Argentina         | 25408 | Centro Cientifico Tecnologico Bahia Blanca |      8 |     21 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['geocodigoFundar', 'geonombreFundar', 'idp', 'institucion'], names_to_col='anio', values_to_col='ranking')
#  RangeIndex: 38 entries, 0 to 37
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  38 non-null     object 
#   1   geonombreFundar  38 non-null     object 
#   2   idp              38 non-null     int64  
#   3   institucion      38 non-null     object 
#   4   anio             38 non-null     object 
#   5   ranking          38 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   idp | institucion                                |   anio |   ranking |
#  |---:|:------------------|:------------------|------:|:-------------------------------------------|-------:|----------:|
#  |  0 | ARG               | Argentina         | 25408 | Centro Cientifico Tecnologico Bahia Blanca |   2009 |         8 |
#  
#  ------------------------------
#  