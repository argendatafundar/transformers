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
	rename_cols(map={'microd_name': 'categoria', 'export_value_pc': 'valor'}),
	drop_col(col=['country_name_abbreviation', 'microd'], axis=1)
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
#   5   export_value_pc            452 non-null    float64
#  
#  |    |   year | iso3   | country_name_abbreviation   |   microd | microd_name   |   export_value_pc |
#  |---:|-------:|:-------|:----------------------------|---------:|:--------------|------------------:|
#  |  0 |   2020 | ARM    | Armenia                     |        1 | Diferenciado  |           30.7665 |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 2 entries, 212 to 213
#  Data columns (total 6 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   year                       2 non-null      int64  
#   1   iso3                       2 non-null      object 
#   2   country_name_abbreviation  2 non-null      object 
#   3   microd                     2 non-null      int64  
#   4   microd_name                2 non-null      object 
#   5   export_value_pc            2 non-null      float64
#  
#  |     |   year | iso3   | country_name_abbreviation   |   microd | microd_name   |   export_value_pc |
#  |----:|-------:|:-------|:----------------------------|---------:|:--------------|------------------:|
#  | 212 |   2020 | ARG    | Argentina                   |        1 | Diferenciado  |           24.6849 |
#  
#  ------------------------------
#  
#  rename_cols(map={'microd_name': 'categoria', 'export_value_pc': 'valor'})
#  Index: 2 entries, 212 to 213
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
#  |     |   year | iso3   | country_name_abbreviation   |   microd | categoria    |   valor |
#  |----:|-------:|:-------|:----------------------------|---------:|:-------------|--------:|
#  | 212 |   2020 | ARG    | Argentina                   |        1 | Diferenciado | 24.6849 |
#  
#  ------------------------------
#  
#  drop_col(col=['country_name_abbreviation', 'microd'], axis=1)
#  Index: 2 entries, 212 to 213
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   year       2 non-null      int64  
#   1   iso3       2 non-null      object 
#   2   categoria  2 non-null      object 
#   3   valor      2 non-null      float64
#  
#  |     |   year | iso3   | categoria    |   valor |
#  |----:|-------:|:-------|:-------------|--------:|
#  | 212 |   2020 | ARG    | Diferenciado | 24.6849 |
#  
#  ------------------------------
#  