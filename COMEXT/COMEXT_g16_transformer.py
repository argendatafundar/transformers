from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='iso3 == "ARG"'),
	drop_col(col='iso3', axis=1),
	drop_col(col='location_name_short_en', axis=1),
	drop_col(col='partner_region_id', axis=1),
	rename_cols(map={'year': 'anio', 'partner_region_name_short_es': 'indicador', 'export_value_pc': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 75560 entries, 0 to 75559
#  Data columns (total 6 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   year                          75560 non-null  int64  
#   1   iso3                          75560 non-null  object 
#   2   location_name_short_en        75560 non-null  object 
#   3   partner_region_id             75560 non-null  int64  
#   4   partner_region_name_short_es  75560 non-null  object 
#   5   export_value_pc               75321 non-null  float64
#  
#  |    |   year | iso3   | location_name_short_en   |   partner_region_id | partner_region_name_short_es   |   export_value_pc |
#  |---:|-------:|:-------|:-------------------------|--------------------:|:-------------------------------|------------------:|
#  |  0 |   2005 | ABW    | Aruba                    |                 356 | África                         |        0.00608911 |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 415 entries, 8 to 75090
#  Data columns (total 6 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   year                          415 non-null    int64  
#   1   iso3                          415 non-null    object 
#   2   location_name_short_en        415 non-null    object 
#   3   partner_region_id             415 non-null    int64  
#   4   partner_region_name_short_es  415 non-null    object 
#   5   export_value_pc               415 non-null    float64
#  
#  |    |   year | iso3   | location_name_short_en   |   partner_region_id | partner_region_name_short_es   |   export_value_pc |
#  |---:|-------:|:-------|:-------------------------|--------------------:|:-------------------------------|------------------:|
#  |  8 |   2009 | ARG    | Argentina                |                 357 | África                         |           5.48451 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 415 entries, 8 to 75090
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   year                          415 non-null    int64  
#   1   location_name_short_en        415 non-null    object 
#   2   partner_region_id             415 non-null    int64  
#   3   partner_region_name_short_es  415 non-null    object 
#   4   export_value_pc               415 non-null    float64
#  
#  |    |   year | location_name_short_en   |   partner_region_id | partner_region_name_short_es   |   export_value_pc |
#  |---:|-------:|:-------------------------|--------------------:|:-------------------------------|------------------:|
#  |  8 |   2009 | Argentina                |                 357 | África                         |           5.48451 |
#  
#  ------------------------------
#  
#  drop_col(col='location_name_short_en', axis=1)
#  Index: 415 entries, 8 to 75090
#  Data columns (total 4 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   year                          415 non-null    int64  
#   1   partner_region_id             415 non-null    int64  
#   2   partner_region_name_short_es  415 non-null    object 
#   3   export_value_pc               415 non-null    float64
#  
#  |    |   year |   partner_region_id | partner_region_name_short_es   |   export_value_pc |
#  |---:|-------:|--------------------:|:-------------------------------|------------------:|
#  |  8 |   2009 |                 357 | África                         |           5.48451 |
#  
#  ------------------------------
#  
#  drop_col(col='partner_region_id', axis=1)
#  Index: 415 entries, 8 to 75090
#  Data columns (total 3 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   year                          415 non-null    int64  
#   1   partner_region_name_short_es  415 non-null    object 
#   2   export_value_pc               415 non-null    float64
#  
#  |    |   year | partner_region_name_short_es   |   export_value_pc |
#  |---:|-------:|:-------------------------------|------------------:|
#  |  8 |   2009 | África                         |           5.48451 |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'partner_region_name_short_es': 'indicador', 'export_value_pc': 'valor'})
#  Index: 415 entries, 8 to 75090
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       415 non-null    int64  
#   1   indicador  415 non-null    object 
#   2   valor      415 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  8 |   2009 | África      | 5.48451 |
#  
#  ------------------------------
#  