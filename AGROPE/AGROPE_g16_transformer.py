from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_na(col=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigoFundar']),
	query(condition="geocodigoFundar != 'F351'"),
	multiplicar_por_escalar(col='valor', k=1e-06)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 9551 entries, 0 to 9550
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  9551 non-null   object 
#   1   geonombreFundar  9551 non-null   object 
#   2   anio             9551 non-null   int64  
#   3   valor            9551 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AFG               | Afganistán        |   1961 |  700000 |
#  
#  ------------------------------
#  
#  drop_na(col=['valor'])
#  RangeIndex: 9551 entries, 0 to 9550
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  9551 non-null   object 
#   1   geonombreFundar  9551 non-null   object 
#   2   anio             9551 non-null   int64  
#   3   valor            9551 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AFG               | Afganistán        |   1961 |  700000 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigoFundar'])
#  RangeIndex: 9551 entries, 0 to 9550
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  9551 non-null   object 
#   1   geonombreFundar  9551 non-null   object 
#   2   anio             9551 non-null   int64  
#   3   valor            9551 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AFG               | Afganistán        |   1961 |  700000 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar != 'F351'")
#  Index: 9489 entries, 0 to 9550
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  9489 non-null   object 
#   1   geonombreFundar  9489 non-null   object 
#   2   anio             9489 non-null   int64  
#   3   valor            9489 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AFG               | Afganistán        |   1961 |     0.7 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  Index: 9489 entries, 0 to 9550
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  9489 non-null   object 
#   1   geonombreFundar  9489 non-null   object 
#   2   anio             9489 non-null   int64  
#   3   valor            9489 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AFG               | Afganistán        |   1961 |     0.7 |
#  
#  ------------------------------
#  