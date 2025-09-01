from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_na(df:DataFrame, cols:list):
    df = df.dropna(subset=cols, axis=0)
    return df

@transformer.convert
def fill_mundo_world(df: DataFrame, dummy = True) -> DataFrame:
    df.loc[
        (df['country'] == 'World') & df['geonombreFundar'].isna(),
        'geonombreFundar'
    ] = 'Mundo'
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'gdi': 'valor'}),
	fill_mundo_world(dummy=True),
	drop_na(cols=['valor']),
	query(condition='anio in [1990, 2006, 2022]')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
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
#   1   geonombreFundar  6798 non-null   object 
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
#  fill_mundo_world(dummy=True)
#  RangeIndex: 6798 entries, 0 to 6797
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  6765 non-null   object 
#   1   geonombreFundar  6798 non-null   object 
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
#  drop_na(cols=['valor'])
#  Index: 5014 entries, 18 to 6797
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  4981 non-null   object 
#   1   geonombreFundar  5014 non-null   object 
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
#  query(condition='anio in [1990, 2006, 2022]')
#  Index: 438 entries, 32 to 6797
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  435 non-null    object 
#   1   geonombreFundar  438 non-null    object 
#   2   anio             438 non-null    int64  
#   3   country          438 non-null    object 
#   4   valor            438 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | country     |   valor |
#  |---:|:------------------|:------------------|-------:|:------------|--------:|
#  | 32 | AFG               | Afganistán        |   2022 | Afghanistan |   0.622 |
#  
#  ------------------------------
#  