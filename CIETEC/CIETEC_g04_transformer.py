from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def concatenar_columnas(df:DataFrame, cols:list, nueva_col:str, separtor:str = "-"):
    df[nueva_col] = df[cols].astype(str).agg(separtor.join, axis=1)
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
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
def agregar_parentesis(df: DataFrame, col:str) -> DataFrame:
    import pandas as pd
    df[col] = df[col].apply(lambda x: f"({x})" if pd.notna(x) else x)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def long_to_wide(df:DataFrame, index:list[str], columns:str, values:str):
    df = df.pivot(index=index, columns=columns, values=values).reset_index()
    df.index.name = None
    df.columns.name = None
    df.columns = [str(col) for col in df.columns]  # Convertir columnas a str
    return df  
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio in [2009, 2024]'),
	long_to_wide(index=['geocodigoFundar', 'geonombreFundar', 'idp', 'institucion'], columns='anio', values='ranking'),
	drop_na(col='2009'),
	drop_na(col='2024'),
	pivot_longer(id_cols=['geocodigoFundar', 'geonombreFundar', 'idp', 'institucion'], names_to_col='anio', values_to_col='ranking'),
	agregar_parentesis(col='geocodigoFundar'),
	replace_multiple_values(col='institucion', replacements={'Comision Nacional de Energia Atomica Argentina': 'CNEA', 'Consejo Nacional de Investigaciones Cientificas y Tecnicas': 'CONICET', 'Instituto Nacional de Tecnologia Agropecuaria': 'INTA', 'Empresa Brasileira de Pesquisa Agropecuaria': 'EMBRAPA', 'Instituto Nacional de Pesquisas da Amazonia': 'INPA', 'Instituto Nacional de Pesquisas Espaciais': 'INPE', 'Instituto Mexicano del Petroleo': 'IMP', 'Centro Cientifico Tecnologico Bahia Blanca': 'CCT Bahía Blanca', 'Centro Cientifico Tecnologico Mendoza': 'CCT Mendoza', 'Centro Cientifico Tecnologico La Plata': 'CCT La Plata', 'Instituto Smithsonian de Investigaciones Tropicales, Panama': 'STRI', 'Instituto de Investigaciones FisicoQuimicas, Teoricas y Aplicadas': 'INIFTA', 'Comision de Investigaciones Cientificas': 'CIC', 'Comissao Nacional de Energia Nuclear': 'CNEN', 'Conselho Nacional de Desenvolvimento Cientifico e Tecnologico': 'CNPq', 'Centro de Investigacion en Alimentacion y Desarrollo': 'CIAD', 'Consejo Nacional de Humanidades, Ciencias y Tecnologias': 'CONAHCYT', 'Instituto Nacional de Investigaciones Forestales, Agricolas y Pecuarias': 'INIFAP'}),
	concatenar_columnas(cols=['institucion', 'geocodigoFundar'], nueva_col='institucion_geocodigo', separtor=' '),
	drop_col(col=['geocodigoFundar', 'institucion', 'geonombreFundar', 'idp'], axis=1)
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
#  Data columns (total 7 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   geocodigoFundar        38 non-null     object 
#   1   geonombreFundar        38 non-null     object 
#   2   idp                    38 non-null     int64  
#   3   institucion            38 non-null     object 
#   4   anio                   38 non-null     object 
#   5   ranking                38 non-null     float64
#   6   institucion_geocodigo  38 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   idp | institucion      |   anio |   ranking | institucion_geocodigo   |
#  |---:|:------------------|:------------------|------:|:-----------------|-------:|----------:|:------------------------|
#  |  0 | (ARG)             | Argentina         | 25408 | CCT Bahía Blanca |   2009 |         8 | CCT Bahía Blanca (ARG)  |
#  
#  ------------------------------
#  
#  agregar_parentesis(col='geocodigoFundar')
#  RangeIndex: 38 entries, 0 to 37
#  Data columns (total 7 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   geocodigoFundar        38 non-null     object 
#   1   geonombreFundar        38 non-null     object 
#   2   idp                    38 non-null     int64  
#   3   institucion            38 non-null     object 
#   4   anio                   38 non-null     object 
#   5   ranking                38 non-null     float64
#   6   institucion_geocodigo  38 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   idp | institucion      |   anio |   ranking | institucion_geocodigo   |
#  |---:|:------------------|:------------------|------:|:-----------------|-------:|----------:|:------------------------|
#  |  0 | (ARG)             | Argentina         | 25408 | CCT Bahía Blanca |   2009 |         8 | CCT Bahía Blanca (ARG)  |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='institucion', replacements={'Comision Nacional de Energia Atomica Argentina': 'CNEA', 'Consejo Nacional de Investigaciones Cientificas y Tecnicas': 'CONICET', 'Instituto Nacional de Tecnologia Agropecuaria': 'INTA', 'Empresa Brasileira de Pesquisa Agropecuaria': 'EMBRAPA', 'Instituto Nacional de Pesquisas da Amazonia': 'INPA', 'Instituto Nacional de Pesquisas Espaciais': 'INPE', 'Instituto Mexicano del Petroleo': 'IMP', 'Centro Cientifico Tecnologico Bahia Blanca': 'CCT Bahía Blanca', 'Centro Cientifico Tecnologico Mendoza': 'CCT Mendoza', 'Centro Cientifico Tecnologico La Plata': 'CCT La Plata', 'Instituto Smithsonian de Investigaciones Tropicales, Panama': 'STRI', 'Instituto de Investigaciones FisicoQuimicas, Teoricas y Aplicadas': 'INIFTA', 'Comision de Investigaciones Cientificas': 'CIC', 'Comissao Nacional de Energia Nuclear': 'CNEN', 'Conselho Nacional de Desenvolvimento Cientifico e Tecnologico': 'CNPq', 'Centro de Investigacion en Alimentacion y Desarrollo': 'CIAD', 'Consejo Nacional de Humanidades, Ciencias y Tecnologias': 'CONAHCYT', 'Instituto Nacional de Investigaciones Forestales, Agricolas y Pecuarias': 'INIFAP'})
#  RangeIndex: 38 entries, 0 to 37
#  Data columns (total 7 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   geocodigoFundar        38 non-null     object 
#   1   geonombreFundar        38 non-null     object 
#   2   idp                    38 non-null     int64  
#   3   institucion            38 non-null     object 
#   4   anio                   38 non-null     object 
#   5   ranking                38 non-null     float64
#   6   institucion_geocodigo  38 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   idp | institucion      |   anio |   ranking | institucion_geocodigo   |
#  |---:|:------------------|:------------------|------:|:-----------------|-------:|----------:|:------------------------|
#  |  0 | (ARG)             | Argentina         | 25408 | CCT Bahía Blanca |   2009 |         8 | CCT Bahía Blanca (ARG)  |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['institucion', 'geocodigoFundar'], nueva_col='institucion_geocodigo', separtor=' ')
#  RangeIndex: 38 entries, 0 to 37
#  Data columns (total 7 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   geocodigoFundar        38 non-null     object 
#   1   geonombreFundar        38 non-null     object 
#   2   idp                    38 non-null     int64  
#   3   institucion            38 non-null     object 
#   4   anio                   38 non-null     object 
#   5   ranking                38 non-null     float64
#   6   institucion_geocodigo  38 non-null     object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   idp | institucion      |   anio |   ranking | institucion_geocodigo   |
#  |---:|:------------------|:------------------|------:|:-----------------|-------:|----------:|:------------------------|
#  |  0 | (ARG)             | Argentina         | 25408 | CCT Bahía Blanca |   2009 |         8 | CCT Bahía Blanca (ARG)  |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'institucion', 'geonombreFundar', 'idp'], axis=1)
#  RangeIndex: 38 entries, 0 to 37
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   38 non-null     object 
#   1   ranking                38 non-null     float64
#   2   institucion_geocodigo  38 non-null     object 
#  
#  |    |   anio |   ranking | institucion_geocodigo   |
#  |---:|-------:|----------:|:------------------------|
#  |  0 |   2009 |         8 | CCT Bahía Blanca (ARG)  |
#  
#  ------------------------------
#  