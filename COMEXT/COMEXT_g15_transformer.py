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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='geocodigoFundar == "ARG"'),
	rename_cols(map={'microd_name': 'categoria', 'import_value_pc': 'valor'}),
	drop_col(col=['country_name_abbreviation', 'microd', 'year', 'geocodigoFundar'], axis=1)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 452 entries, 0 to 451
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   geocodigoFundar            452 non-null    object 
#   1   geonombreFundar            452 non-null    object 
#   2   year                       452 non-null    int64  
#   3   country_name_abbreviation  452 non-null    object 
#   4   microd                     452 non-null    int64  
#   5   microd_name                452 non-null    object 
#   6   import_value_pc            452 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | country_name_abbreviation   |   microd | microd_name   |   import_value_pc |
#  |---:|:------------------|:------------------|-------:|:----------------------------|---------:|:--------------|------------------:|
#  |  0 | ABW               | Aruba             |   2020 | Aruba                       |        1 | Diferenciado  |           77.3215 |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar == "ARG"')
#  Index: 2 entries, 14 to 15
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   geocodigoFundar            2 non-null      object 
#   1   geonombreFundar            2 non-null      object 
#   2   year                       2 non-null      int64  
#   3   country_name_abbreviation  2 non-null      object 
#   4   microd                     2 non-null      int64  
#   5   microd_name                2 non-null      object 
#   6   import_value_pc            2 non-null      float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | country_name_abbreviation   |   microd | microd_name   |   import_value_pc |
#  |---:|:------------------|:------------------|-------:|:----------------------------|---------:|:--------------|------------------:|
#  | 14 | ARG               | Argentina         |   2020 | Argentina                   |        1 | Diferenciado  |           66.5803 |
#  
#  ------------------------------
#  
#  rename_cols(map={'microd_name': 'categoria', 'import_value_pc': 'valor'})
#  Index: 2 entries, 14 to 15
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   geocodigoFundar            2 non-null      object 
#   1   geonombreFundar            2 non-null      object 
#   2   year                       2 non-null      int64  
#   3   country_name_abbreviation  2 non-null      object 
#   4   microd                     2 non-null      int64  
#   5   categoria                  2 non-null      object 
#   6   valor                      2 non-null      float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | country_name_abbreviation   |   microd | categoria    |   valor |
#  |---:|:------------------|:------------------|-------:|:----------------------------|---------:|:-------------|--------:|
#  | 14 | ARG               | Argentina         |   2020 | Argentina                   |        1 | Diferenciado | 66.5803 |
#  
#  ------------------------------
#  
#  drop_col(col=['country_name_abbreviation', 'microd', 'year', 'geocodigoFundar'], axis=1)
#  Index: 2 entries, 14 to 15
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  2 non-null      object 
#   1   categoria        2 non-null      object 
#   2   valor            2 non-null      float64
#  
#  |    | geonombreFundar   | categoria    |   valor |
#  |---:|:------------------|:-------------|--------:|
#  | 14 | Argentina         | Diferenciado | 66.5803 |
#  
#  ------------------------------
#  