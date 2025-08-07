from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_na(df: DataFrame, col:str):
    df = df.dropna(subset= col, axis=0)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'geocodigoFundar': 'geocodigo'}),
	drop_col(col='countryname', axis=1),
	rename_cols(map={'exportvolumeindex': 'valor'}),
	drop_na(col='valor'),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    16960 non-null  object 
#   1   geonombreFundar    16960 non-null  object 
#   2   anio               16960 non-null  int64  
#   3   countryname        16960 non-null  object 
#   4   exportvolumeindex  6218 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | countryname   |   exportvolumeindex |
#  |---:|:------------------|:------------------|-------:|:--------------|--------------------:|
#  |  0 | ABW               | Aruba             |   2023 | Aruba         |                 nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'geocodigoFundar': 'geocodigo'})
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigo          16960 non-null  object 
#   1   geonombreFundar    16960 non-null  object 
#   2   anio               16960 non-null  int64  
#   3   countryname        16960 non-null  object 
#   4   exportvolumeindex  6218 non-null   float64
#  
#  |    | geocodigo   | geonombreFundar   |   anio | countryname   |   exportvolumeindex |
#  |---:|:------------|:------------------|-------:|:--------------|--------------------:|
#  |  0 | ABW         | Aruba             |   2023 | Aruba         |                 nan |
#  
#  ------------------------------
#  
#  drop_col(col='countryname', axis=1)
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigo          16960 non-null  object 
#   1   geonombreFundar    16960 non-null  object 
#   2   anio               16960 non-null  int64  
#   3   exportvolumeindex  6218 non-null   float64
#  
#  |    | geocodigo   | geonombreFundar   |   anio |   exportvolumeindex |
#  |---:|:------------|:------------------|-------:|--------------------:|
#  |  0 | ABW         | Aruba             |   2023 |                 nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'exportvolumeindex': 'valor'})
#  RangeIndex: 16960 entries, 0 to 16959
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        16960 non-null  object 
#   1   geonombreFundar  16960 non-null  object 
#   2   anio             16960 non-null  int64  
#   3   valor            6218 non-null   float64
#  
#  |    | geocodigo   | geonombreFundar   |   anio |   valor |
#  |---:|:------------|:------------------|-------:|--------:|
#  |  0 | ABW         | Aruba             |   2023 |     nan |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  Index: 6218 entries, 2 to 16959
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        6218 non-null   object 
#   1   geonombreFundar  6218 non-null   object 
#   2   anio             6218 non-null   int64  
#   3   valor            6218 non-null   float64
#  
#  |    | geocodigo   | geonombreFundar   |   anio |   valor |
#  |---:|:------------|:------------------|-------:|--------:|
#  |  2 | ABW         | Aruba             |   2021 | 2.02195 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 6218 entries, 0 to 6217
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        6218 non-null   object 
#   1   geonombreFundar  6218 non-null   object 
#   2   anio             6218 non-null   int64  
#   3   valor            6218 non-null   float64
#  
#  |    | geocodigo   | geonombreFundar        |   anio |   valor |
#  |---:|:------------|:-----------------------|-------:|--------:|
#  |  0 | ARE         | Emiratos Árabes Unidos |   1980 | 39.4178 |
#  
#  ------------------------------
#  