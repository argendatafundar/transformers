from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
query(condition='iso3 == "ARG"'),
	rename_cols(map={'microd_name': 'categoria', 'import_value_pc': 'valor'}),
	drop_col(col=['country_name_abbreviation', 'microd', 'year', 'iso3'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 452 entries, 0 to 451
#  Data columns (total 6 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   year                       452 non-null    int64  
#   1   iso3                       452 non-null    object 
#   2   country_name_abbreviation  452 non-null    object 
#   3   microd                     452 non-null    int64  
#   4   microd_name                452 non-null    object 
#   5   import_value_pc            452 non-null    float64
#  
#  |    |   year | iso3   | country_name_abbreviation   |   microd | microd_name   |   import_value_pc |
#  |---:|-------:|:-------|:----------------------------|---------:|:--------------|------------------:|
#  |  0 |   2020 | ABW    | Aruba                       |        1 | Diferenciado  |           77.3215 |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 2 entries, 14 to 15
#  Data columns (total 6 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   year                       2 non-null      int64  
#   1   iso3                       2 non-null      object 
#   2   country_name_abbreviation  2 non-null      object 
#   3   microd                     2 non-null      int64  
#   4   microd_name                2 non-null      object 
#   5   import_value_pc            2 non-null      float64
#  
#  |    |   year | iso3   | country_name_abbreviation   |   microd | microd_name   |   import_value_pc |
#  |---:|-------:|:-------|:----------------------------|---------:|:--------------|------------------:|
#  | 14 |   2020 | ARG    | Argentina                   |        1 | Diferenciado  |           66.5803 |
#  
#  ------------------------------
#  
#  rename_cols(map={'microd_name': 'categoria', 'import_value_pc': 'valor'})
#  Index: 2 entries, 14 to 15
#  Data columns (total 6 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   year                       2 non-null      int64  
#   1   iso3                       2 non-null      object 
#   2   country_name_abbreviation  2 non-null      object 
#   3   microd                     2 non-null      int64  
#   4   categoria                  2 non-null      object 
#   5   valor                      2 non-null      float64
#  
#  |    |   year | iso3   | country_name_abbreviation   |   microd | categoria    |   valor |
#  |---:|-------:|:-------|:----------------------------|---------:|:-------------|--------:|
#  | 14 |   2020 | ARG    | Argentina                   |        1 | Diferenciado | 66.5803 |
#  
#  ------------------------------
#  
#  drop_col(col=['country_name_abbreviation', 'microd', 'year', 'iso3'], axis=1)
#  Index: 2 entries, 14 to 15
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  2 non-null      object 
#   1   valor      2 non-null      float64
#  
#  |    | categoria    |   valor |
#  |---:|:-------------|--------:|
#  | 14 | Diferenciado | 66.5803 |
#  
#  ------------------------------
#  