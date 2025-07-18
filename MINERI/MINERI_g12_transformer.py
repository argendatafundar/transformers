from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def str_to_title(df: DataFrame, col:str):
    df[col] = df[col].str.title()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'exportaciones': 'indicador', 'fob': 'valor'}),
	str_to_title(col='indicador'),
	sort_values(how='ascending', by=['geonombreFundar', 'anio', 'indicador'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  250 non-null    object
#   1   geonombreFundar  250 non-null    object
#   2   anio             250 non-null    int64 
#   3   exportaciones    250 non-null    object
#   4   fob              250 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | exportaciones   |       fob |
#  |---:|:------------------|:------------------|-------:|:----------------|----------:|
#  |  0 | AR-K              | Catamarca         |   1998 | mineras         | 438881324 |
#  
#  ------------------------------
#  
#  rename_cols(map={'exportaciones': 'indicador', 'fob': 'valor'})
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  250 non-null    object
#   1   geonombreFundar  250 non-null    object
#   2   anio             250 non-null    int64 
#   3   indicador        250 non-null    object
#   4   valor            250 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | indicador   |     valor |
#  |---:|:------------------|:------------------|-------:|:------------|----------:|
#  |  0 | AR-K              | Catamarca         |   1998 | Mineras     | 438881324 |
#  
#  ------------------------------
#  
#  str_to_title(col='indicador')
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  250 non-null    object
#   1   geonombreFundar  250 non-null    object
#   2   anio             250 non-null    int64 
#   3   indicador        250 non-null    object
#   4   valor            250 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | indicador   |     valor |
#  |---:|:------------------|:------------------|-------:|:------------|----------:|
#  |  0 | AR-K              | Catamarca         |   1998 | Mineras     | 438881324 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['geonombreFundar', 'anio', 'indicador'])
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype 
#  ---  ------           --------------  ----- 
#   0   geocodigoFundar  250 non-null    object
#   1   geonombreFundar  250 non-null    object
#   2   anio             250 non-null    int64 
#   3   indicador        250 non-null    object
#   4   valor            250 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | indicador   |     valor |
#  |---:|:------------------|:------------------|-------:|:------------|----------:|
#  |  0 | AR-K              | Catamarca         |   1998 | Mineras     | 438881324 |
#  
#  ------------------------------
#  