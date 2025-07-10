from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 81 entries, 0 to 80
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype 
#  ---  ------            --------------  ----- 
#   0   geocodigoFundar   81 non-null     object
#   1   geonombreFundar   81 non-null     object
#   2   anio              81 non-null     int64 
#   3   funcion           81 non-null     object
#   4   personas_fisicas  81 non-null     int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | funcion        |   personas_fisicas |
#  |---:|:------------------|:------------------|-------:|:---------------|-------------------:|
#  |  0 | ARG               | Argentina         |   2017 | Investigadores |              84284 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 81 entries, 0 to 80
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype 
#  ---  ------            --------------  ----- 
#   0   geocodigoFundar   81 non-null     object
#   1   geonombreFundar   81 non-null     object
#   2   anio              81 non-null     int64 
#   3   funcion           81 non-null     object
#   4   personas_fisicas  81 non-null     int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | funcion        |   personas_fisicas |
#  |---:|:------------------|:------------------|-------:|:---------------|-------------------:|
#  |  0 | ARG               | Argentina         |   1997 | Investigadores |              37198 |
#  
#  ------------------------------
#  