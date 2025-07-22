from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio in [1997, 1999, 2001, 2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021, 2023]'),
	sort_values(how='ascending', by=['anio']),
	replace_multiple_values(col='funcion', replacements={'Técnicos y Personal Asimilado': 'P. técnico', 'Otro Personal de Apoyo': 'P. de apoyo'})
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
#  query(condition='anio in [1997, 1999, 2001, 2003, 2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021, 2023]')
#  Index: 42 entries, 0 to 79
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype 
#  ---  ------            --------------  ----- 
#   0   geocodigoFundar   42 non-null     object
#   1   geonombreFundar   42 non-null     object
#   2   anio              42 non-null     int64 
#   3   funcion           42 non-null     object
#   4   personas_fisicas  42 non-null     int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | funcion        |   personas_fisicas |
#  |---:|:------------------|:------------------|-------:|:---------------|-------------------:|
#  |  0 | ARG               | Argentina         |   2017 | Investigadores |              84284 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype 
#  ---  ------            --------------  ----- 
#   0   geocodigoFundar   42 non-null     object
#   1   geonombreFundar   42 non-null     object
#   2   anio              42 non-null     int64 
#   3   funcion           42 non-null     object
#   4   personas_fisicas  42 non-null     int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | funcion        |   personas_fisicas |
#  |---:|:------------------|:------------------|-------:|:---------------|-------------------:|
#  |  0 | ARG               | Argentina         |   1997 | Investigadores |              37198 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='funcion', replacements={'Técnicos y Personal Asimilado': 'P. técnico', 'Otro Personal de Apoyo': 'P. de apoyo'})
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype 
#  ---  ------            --------------  ----- 
#   0   geocodigoFundar   42 non-null     object
#   1   geonombreFundar   42 non-null     object
#   2   anio              42 non-null     int64 
#   3   funcion           42 non-null     object
#   4   personas_fisicas  42 non-null     int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | funcion        |   personas_fisicas |
#  |---:|:------------------|:------------------|-------:|:---------------|-------------------:|
#  |  0 | ARG               | Argentina         |   1997 | Investigadores |              37198 |
#  
#  ------------------------------
#  