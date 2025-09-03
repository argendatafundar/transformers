from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'expectativa_educ': 'valor'}),
	drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1),
	replace_value(col=None, curr_value=None, new_value=None, mapping={'geonombreFundar': {'Países de desarrollo medio': 'P. de desarrollo medio'}})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 6550 entries, 0 to 6549
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6550 non-null   object 
#   1   geonombreFundar    6550 non-null   object 
#   2   continente_fundar  6187 non-null   object 
#   3   es_agregacion      6187 non-null   float64
#   4   anio               6550 non-null   int64  
#   5   expectativa_educ   6550 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   expectativa_educ |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|-------------------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 |               2.94 |
#  
#  ------------------------------
#  
#  rename_cols(map={'expectativa_educ': 'valor'})
#  RangeIndex: 6550 entries, 0 to 6549
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6550 non-null   object 
#   1   geonombreFundar    6550 non-null   object 
#   2   continente_fundar  6187 non-null   object 
#   3   es_agregacion      6187 non-null   float64
#   4   anio               6550 non-null   int64  
#   5   valor              6550 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   valor |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|--------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 |    2.94 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1)
#  RangeIndex: 6550 entries, 0 to 6549
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  6550 non-null   object 
#   1   anio             6550 non-null   int64  
#   2   valor            6550 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|-------:|--------:|
#  |  0 | Afganistán        |   1990 |    2.94 |
#  
#  ------------------------------
#  
#  replace_value(col=None, curr_value=None, new_value=None, mapping={'geonombreFundar': {'Países de desarrollo medio': 'P. de desarrollo medio'}})
#  RangeIndex: 6550 entries, 0 to 6549
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  6550 non-null   object 
#   1   anio             6550 non-null   int64  
#   2   valor            6550 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|-------:|--------:|
#  |  0 | Afganistán        |   1990 |    2.94 |
#  
#  ------------------------------
#  