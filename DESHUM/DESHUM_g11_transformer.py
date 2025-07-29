from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'gdi': 'valor'}),
	drop_na(col=['valor'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6798 entries, 0 to 6797
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6765 non-null   object 
#   1   geonombreFundar  6765 non-null   object 
#   2   anio             6798 non-null   int64  
#   3   country          6798 non-null   object 
#   4   gdi              5014 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | country     |   gdi |
#  |---:|:------------------|:------------------|-------:|:------------|------:|
#  |  0 | AFG               | Afganistán        |   1990 | Afghanistan |   nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'gdi': 'valor'})
#  RangeIndex: 6798 entries, 0 to 6797
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6765 non-null   object 
#   1   geonombreFundar  6765 non-null   object 
#   2   anio             6798 non-null   int64  
#   3   country          6798 non-null   object 
#   4   valor            5014 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | country     |   valor |
#  |---:|:------------------|:------------------|-------:|:------------|--------:|
#  |  0 | AFG               | Afganistán        |   1990 | Afghanistan |     nan |
#  
#  ------------------------------
#  
#  drop_na(col=['valor'])
#  Index: 5014 entries, 18 to 6797
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  4981 non-null   object 
#   1   geonombreFundar  4981 non-null   object 
#   2   anio             5014 non-null   int64  
#   3   country          5014 non-null   object 
#   4   valor            5014 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | country     |   valor |
#  |---:|:------------------|:------------------|-------:|:------------|--------:|
#  | 18 | AFG               | Afganistán        |   2008 | Afghanistan |   0.682 |
#  
#  ------------------------------
#  