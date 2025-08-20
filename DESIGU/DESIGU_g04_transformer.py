from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
def drop_na(df, subset:str): 
    return df.dropna(subset=subset, axis=0)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'gini': 'valor', 'code': 'geocodigo'}),
	drop_col(col='pais', axis=1),
	drop_na(subset=['valor']),
	sort_values(how='ascending', by=['valor'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  162 non-null    object 
#   1   geonombreFundar  162 non-null    object 
#   2   pais             162 non-null    object 
#   3   gini             153 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | pais    |   gini |
#  |---:|:------------------|:------------------|:--------|-------:|
#  |  0 | ISL               | Islandia          | Iceland |   24.3 |
#  
#  ------------------------------
#  
#  rename_cols(map={'gini': 'valor', 'code': 'geocodigo'})
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  162 non-null    object 
#   1   geonombreFundar  162 non-null    object 
#   2   pais             162 non-null    object 
#   3   valor            153 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | pais    |   valor |
#  |---:|:------------------|:------------------|:--------|--------:|
#  |  0 | ISL               | Islandia          | Iceland |    24.3 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  162 non-null    object 
#   1   geonombreFundar  162 non-null    object 
#   2   valor            153 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   valor |
#  |---:|:------------------|:------------------|--------:|
#  |  0 | ISL               | Islandia          |    24.3 |
#  
#  ------------------------------
#  
#  drop_na(subset=['valor'])
#  Index: 153 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  153 non-null    object 
#   1   geonombreFundar  153 non-null    object 
#   2   valor            153 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   valor |
#  |---:|:------------------|:------------------|--------:|
#  |  0 | ISL               | Islandia          |    24.3 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['valor'])
#  RangeIndex: 153 entries, 0 to 152
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  153 non-null    object 
#   1   geonombreFundar  153 non-null    object 
#   2   valor            153 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   valor |
#  |---:|:------------------|:------------------|--------:|
#  |  0 | ISL               | Islandia          |    24.3 |
#  
#  ------------------------------
#  