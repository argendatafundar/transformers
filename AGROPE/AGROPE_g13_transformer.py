from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
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

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'rindes_trigo_ma5': 'valor'}),
	query(condition='anio >= 1965'),
	drop_na(col=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigoFundar']),
	query(condition="geocodigoFundar != 'F351'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6876 entries, 0 to 6875
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigoFundar   6876 non-null   object 
#   1   geonombreFundar   6876 non-null   object 
#   2   anio              6876 non-null   int64  
#   3   rindes            6876 non-null   float64
#   4   rindes_trigo_ma5  6876 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes | rindes_trigo_ma5   |
#  |---:|:------------------|:------------------|-------:|---------:|:-------------------|
#  |  0 | AFG               | Afganistán        |   1961 |    1.022 | NA                 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rindes_trigo_ma5': 'valor'})
#  RangeIndex: 6876 entries, 0 to 6875
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6876 non-null   object 
#   1   geonombreFundar  6876 non-null   object 
#   2   anio             6876 non-null   int64  
#   3   rindes           6876 non-null   float64
#   4   valor            6876 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes | valor   |
#  |---:|:------------------|:------------------|-------:|---------:|:--------|
#  |  0 | AFG               | Afganistán        |   1961 |    1.022 | NA      |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1965')
#  Index: 6504 entries, 4 to 6875
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6504 non-null   object 
#   1   geonombreFundar  6504 non-null   object 
#   2   anio             6504 non-null   int64  
#   3   rindes           6504 non-null   float64
#   4   valor            6504 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes |   valor |
#  |---:|:------------------|:------------------|-------:|---------:|--------:|
#  |  4 | AFG               | Afganistán        |   1965 |   0.9723 |  0.9501 |
#  
#  ------------------------------
#  
#  drop_na(col=['valor'])
#  Index: 6504 entries, 4 to 6875
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6504 non-null   object 
#   1   geonombreFundar  6504 non-null   object 
#   2   anio             6504 non-null   int64  
#   3   rindes           6504 non-null   float64
#   4   valor            6504 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes |   valor |
#  |---:|:------------------|:------------------|-------:|---------:|--------:|
#  |  4 | AFG               | Afganistán        |   1965 |   0.9723 |  0.9501 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigoFundar'])
#  RangeIndex: 6504 entries, 0 to 6503
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6504 non-null   object 
#   1   geonombreFundar  6504 non-null   object 
#   2   anio             6504 non-null   int64  
#   3   rindes           6504 non-null   float64
#   4   valor            6504 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes |   valor |
#  |---:|:------------------|:------------------|-------:|---------:|--------:|
#  |  0 | AFG               | Afganistán        |   1965 |   0.9723 |  0.9501 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar != 'F351'")
#  Index: 6472 entries, 0 to 6503
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6472 non-null   object 
#   1   geonombreFundar  6472 non-null   object 
#   2   anio             6472 non-null   int64  
#   3   rindes           6472 non-null   float64
#   4   valor            6472 non-null   object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   rindes |   valor |
#  |---:|:------------------|:------------------|-------:|---------:|--------:|
#  |  0 | AFG               | Afganistán        |   1965 |   0.9723 |  0.9501 |
#  
#  ------------------------------
#  