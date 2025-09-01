from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    df = df.dropna(subset=cols, axis=0)
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
	rename_cols(map={'idha': 'valor'}),
	drop_na(cols=['valor']),
	replace_value(col=None, curr_value=None, new_value=None, mapping={'geonombreFundar': {'Ramificaciones de Occidente': 'Ramif. de Occidente'}})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  4176 non-null   object 
#   1   geonombreFundar  4176 non-null   object 
#   2   anio             4176 non-null   int64  
#   3   idha             3685 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   idha |
#  |---:|:------------------|:------------------|-------:|-------:|
#  |  0 | AFG               | Afganistán        |   1870 |    nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'idha': 'valor'})
#  RangeIndex: 4176 entries, 0 to 4175
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  4176 non-null   object 
#   1   geonombreFundar  4176 non-null   object 
#   2   anio             4176 non-null   int64  
#   3   valor            3685 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|:------------------|-------:|--------:|
#  |  0 | AFG               | Afganistán        |   1870 |     nan |
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  Index: 3685 entries, 9 to 4175
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  3685 non-null   object 
#   1   geonombreFundar  3685 non-null   object 
#   2   anio             3685 non-null   int64  
#   3   valor            3685 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |     valor |
#  |---:|:------------------|:------------------|-------:|----------:|
#  |  9 | AFG               | Afganistán        |   1950 | 0.0500429 |
#  
#  ------------------------------
#  
#  replace_value(col=None, curr_value=None, new_value=None, mapping={'geonombreFundar': {'Ramificaciones de Occidente': 'Ramif. de Occidente'}})
#  Index: 3685 entries, 9 to 4175
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  3685 non-null   object 
#   1   geonombreFundar  3685 non-null   object 
#   2   anio             3685 non-null   int64  
#   3   valor            3685 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |     valor |
#  |---:|:------------------|:------------------|-------:|----------:|
#  |  9 | AFG               | Afganistán        |   1950 | 0.0500429 |
#  
#  ------------------------------
#  