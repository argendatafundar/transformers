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
	rename_cols(map={'idha': 'valor'}),
	drop_na(col=['valor'])
)
#  PIPELINE_END


#  start()
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
#  drop_na(col=['valor'])
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