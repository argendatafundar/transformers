from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'share_expo': 'valor'}),
	sort_values(how='ascending', by=['anio', 'geocodigoFundar']),
	query(condition="geocodigoFundar != 'F351'"),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6747 entries, 0 to 6746
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6747 non-null   object 
#   1   geonombreFundar  6747 non-null   object 
#   2   anio             6747 non-null   int64  
#   3   share_expo       6747 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   share_expo |
#  |---:|:------------------|:------------------|-------:|-------------:|
#  |  0 | ABW               | Aruba             |   2014 |            0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'share_expo': 'valor'})
#  RangeIndex: 6747 entries, 0 to 6746
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6747 non-null   object 
#   1   geonombreFundar  6747 non-null   object 
#   2   anio             6747 non-null   int64  
#   3   valor            6747 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | ABW               | Aruba             |   2014 |       0 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigoFundar'])
#  RangeIndex: 6747 entries, 0 to 6746
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6747 non-null   object 
#   1   geonombreFundar  6747 non-null   object 
#   2   anio             6747 non-null   int64  
#   3   valor            6747 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AGO               | Angola            |   1962 |  0.0009 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar != 'F351'")
#  Index: 6747 entries, 0 to 6746
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6747 non-null   object 
#   1   geonombreFundar  6747 non-null   object 
#   2   anio             6747 non-null   int64  
#   3   valor            6747 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AGO               | Angola            |   1962 |    0.09 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 6747 entries, 0 to 6746
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6747 non-null   object 
#   1   geonombreFundar  6747 non-null   object 
#   2   anio             6747 non-null   int64  
#   3   valor            6747 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AGO               | Angola            |   1962 |    0.09 |
#  
#  ------------------------------
#  