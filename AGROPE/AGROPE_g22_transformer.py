from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="geocodigoFundar != 'F351'"),
	multiplicar_por_escalar(col='valor', k=1e-06)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 11363 entries, 0 to 11362
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  11363 non-null  object 
#   1   geonombreFundar  11363 non-null  object 
#   2   anio             11363 non-null  int64  
#   3   valor            11363 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AFG               | Afganistán        |   1961 |   43000 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar != 'F351'")
#  Index: 11363 entries, 0 to 11362
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  11363 non-null  object 
#   1   geonombreFundar  11363 non-null  object 
#   2   anio             11363 non-null  int64  
#   3   valor            11363 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AFG               | Afganistán        |   1961 |   0.043 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  Index: 11363 entries, 0 to 11362
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  11363 non-null  object 
#   1   geonombreFundar  11363 non-null  object 
#   2   anio             11363 non-null  int64  
#   3   valor            11363 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AFG               | Afganistán        |   1961 |   0.043 |
#  
#  ------------------------------
#  