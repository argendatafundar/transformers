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
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

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
	to_pandas(dummy=True),
	query(condition='geocodigoFundar == "ARG"'),
	drop_col(col='geocodigoFundar', axis=1),
	drop_col(col='location_name_short_en', axis=1),
	drop_col(col='partner_region_id', axis=1),
	rename_cols(map={'year': 'anio', 'partner_region_name_short_es': 'indicador', 'export_value_pc': 'valor'}),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 75560 entries, 0 to 75559
#  Data columns (total 8 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   year                          75560 non-null  int64  
#   1   iso3                          75560 non-null  object 
#   2   location_name_short_en        75560 non-null  object 
#   3   partner_region_id             75560 non-null  int64  
#   4   partner_region_name_short_es  75560 non-null  object 
#   5   export_value_pc               75321 non-null  float64
#   6   geocodigoFundar               75560 non-null  object 
#   7   geonombreFundar               75560 non-null  object 
#  
#  |    |   year | iso3   | location_name_short_en   |   partner_region_id | partner_region_name_short_es   |   export_value_pc | geocodigoFundar   | geonombreFundar   |
#  |---:|-------:|:-------|:-------------------------|--------------------:|:-------------------------------|------------------:|:------------------|:------------------|
#  |  0 |   2017 | ABW    | Aruba                    |                 356 | Ámerica del Norte              |           25.6312 | ABW               | Aruba             |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar == "ARG"')
#  Index: 415 entries, 8 to 75090
#  Data columns (total 8 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   year                          415 non-null    int64  
#   1   iso3                          415 non-null    object 
#   2   location_name_short_en        415 non-null    object 
#   3   partner_region_id             415 non-null    int64  
#   4   partner_region_name_short_es  415 non-null    object 
#   5   export_value_pc               415 non-null    float64
#   6   geocodigoFundar               415 non-null    object 
#   7   geonombreFundar               415 non-null    object 
#  
#  |    |   year | iso3   | location_name_short_en   |   partner_region_id | partner_region_name_short_es   |   export_value_pc | geocodigoFundar   | geonombreFundar   |
#  |---:|-------:|:-------|:-------------------------|--------------------:|:-------------------------------|------------------:|:------------------|:------------------|
#  |  8 |   1996 | ARG    | Argentina                |                 357 | Otros                          |          0.225482 | ARG               | Argentina         |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 415 entries, 8 to 75090
#  Data columns (total 7 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   year                          415 non-null    int64  
#   1   iso3                          415 non-null    object 
#   2   location_name_short_en        415 non-null    object 
#   3   partner_region_id             415 non-null    int64  
#   4   partner_region_name_short_es  415 non-null    object 
#   5   export_value_pc               415 non-null    float64
#   6   geonombreFundar               415 non-null    object 
#  
#  |    |   year | iso3   | location_name_short_en   |   partner_region_id | partner_region_name_short_es   |   export_value_pc | geonombreFundar   |
#  |---:|-------:|:-------|:-------------------------|--------------------:|:-------------------------------|------------------:|:------------------|
#  |  8 |   1996 | ARG    | Argentina                |                 357 | Otros                          |          0.225482 | Argentina         |
#  
#  ------------------------------
#  
#  drop_col(col='location_name_short_en', axis=1)
#  Index: 415 entries, 8 to 75090
#  Data columns (total 6 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   year                          415 non-null    int64  
#   1   iso3                          415 non-null    object 
#   2   partner_region_id             415 non-null    int64  
#   3   partner_region_name_short_es  415 non-null    object 
#   4   export_value_pc               415 non-null    float64
#   5   geonombreFundar               415 non-null    object 
#  
#  |    |   year | iso3   |   partner_region_id | partner_region_name_short_es   |   export_value_pc | geonombreFundar   |
#  |---:|-------:|:-------|--------------------:|:-------------------------------|------------------:|:------------------|
#  |  8 |   1996 | ARG    |                 357 | Otros                          |          0.225482 | Argentina         |
#  
#  ------------------------------
#  
#  drop_col(col='partner_region_id', axis=1)
#  Index: 415 entries, 8 to 75090
#  Data columns (total 5 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   year                          415 non-null    int64  
#   1   iso3                          415 non-null    object 
#   2   partner_region_name_short_es  415 non-null    object 
#   3   export_value_pc               415 non-null    float64
#   4   geonombreFundar               415 non-null    object 
#  
#  |    |   year | iso3   | partner_region_name_short_es   |   export_value_pc | geonombreFundar   |
#  |---:|-------:|:-------|:-------------------------------|------------------:|:------------------|
#  |  8 |   1996 | ARG    | Otros                          |          0.225482 | Argentina         |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'partner_region_name_short_es': 'indicador', 'export_value_pc': 'valor'})
#  Index: 415 entries, 8 to 75090
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             415 non-null    int64  
#   1   iso3             415 non-null    object 
#   2   indicador        415 non-null    object 
#   3   valor            415 non-null    float64
#   4   geonombreFundar  415 non-null    object 
#  
#  |    |   anio | iso3   | indicador   |    valor | geonombreFundar   |
#  |---:|-------:|:-------|:------------|---------:|:------------------|
#  |  8 |   1996 | ARG    | Otros       | 0.225482 | Argentina         |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 415 entries, 0 to 414
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             415 non-null    int64  
#   1   iso3             415 non-null    object 
#   2   indicador        415 non-null    object 
#   3   valor            415 non-null    float64
#   4   geonombreFundar  415 non-null    object 
#  
#  |    |   anio | iso3   | indicador   |   valor | geonombreFundar   |
#  |---:|-------:|:-------|:------------|--------:|:------------------|
#  |  0 |   1962 | ARG    | Europa      | 72.7448 | Argentina         |
#  
#  ------------------------------
#  