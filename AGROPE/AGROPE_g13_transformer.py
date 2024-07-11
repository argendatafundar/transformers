from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='F15', new_value='BLX'),
	replace_value(col='iso3', curr_value='F228', new_value='SVU'),
	replace_value(col='iso3', curr_value='F248', new_value='SER'),
	replace_value(col='iso3', curr_value='F51', new_value='CSK'),
	rename_cols(map={'iso3': 'geocodigo', 'rindes_trigo_ma5': 'valor'}),
	drop_col(col='iso3_desc_fundar', axis=1),
	drop_col(col='rindes', axis=1),
	query(condition='anio >= 1965'),
	drop_na(col=['valor']),
	sort_values(how='ascending', by=['anio', 'geocodigo']),
	query(condition="geocodigo != 'F351'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6876 entries, 0 to 6875
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              6876 non-null   object 
#   1   iso3_desc_fundar  6876 non-null   object 
#   2   anio              6876 non-null   int64  
#   3   rindes            6876 non-null   float64
#   4   rindes_trigo_ma5  6876 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_trigo_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:-------------------|
#  |  0 | AFG    | Afganistán         |   1961 |    1.022 | NA                 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F15', new_value='BLX')
#  RangeIndex: 6876 entries, 0 to 6875
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              6876 non-null   object 
#   1   iso3_desc_fundar  6876 non-null   object 
#   2   anio              6876 non-null   int64  
#   3   rindes            6876 non-null   float64
#   4   rindes_trigo_ma5  6876 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_trigo_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:-------------------|
#  |  0 | AFG    | Afganistán         |   1961 |    1.022 | NA                 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F228', new_value='SVU')
#  RangeIndex: 6876 entries, 0 to 6875
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              6876 non-null   object 
#   1   iso3_desc_fundar  6876 non-null   object 
#   2   anio              6876 non-null   int64  
#   3   rindes            6876 non-null   float64
#   4   rindes_trigo_ma5  6876 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_trigo_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:-------------------|
#  |  0 | AFG    | Afganistán         |   1961 |    1.022 | NA                 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F248', new_value='SER')
#  RangeIndex: 6876 entries, 0 to 6875
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              6876 non-null   object 
#   1   iso3_desc_fundar  6876 non-null   object 
#   2   anio              6876 non-null   int64  
#   3   rindes            6876 non-null   float64
#   4   rindes_trigo_ma5  6876 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_trigo_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:-------------------|
#  |  0 | AFG    | Afganistán         |   1961 |    1.022 | NA                 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F51', new_value='CSK')
#  RangeIndex: 6876 entries, 0 to 6875
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              6876 non-null   object 
#   1   iso3_desc_fundar  6876 non-null   object 
#   2   anio              6876 non-null   int64  
#   3   rindes            6876 non-null   float64
#   4   rindes_trigo_ma5  6876 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_trigo_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:-------------------|
#  |  0 | AFG    | Afganistán         |   1961 |    1.022 | NA                 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'rindes_trigo_ma5': 'valor'})
#  RangeIndex: 6876 entries, 0 to 6875
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         6876 non-null   object 
#   1   iso3_desc_fundar  6876 non-null   object 
#   2   anio              6876 non-null   int64  
#   3   rindes            6876 non-null   float64
#   4   valor             6876 non-null   object 
#  
#  |    | geocodigo   | iso3_desc_fundar   |   anio |   rindes | valor   |
#  |---:|:------------|:-------------------|-------:|---------:|:--------|
#  |  0 | AFG         | Afganistán         |   1961 |    1.022 | NA      |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc_fundar', axis=1)
#  RangeIndex: 6876 entries, 0 to 6875
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  6876 non-null   object 
#   1   anio       6876 non-null   int64  
#   2   rindes     6876 non-null   float64
#   3   valor      6876 non-null   object 
#  
#  |    | geocodigo   |   anio |   rindes | valor   |
#  |---:|:------------|-------:|---------:|:--------|
#  |  0 | AFG         |   1961 |    1.022 | NA      |
#  
#  ------------------------------
#  
#  drop_col(col='rindes', axis=1)
#  RangeIndex: 6876 entries, 0 to 6875
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   geocodigo  6876 non-null   object
#   1   anio       6876 non-null   int64 
#   2   valor      6876 non-null   object
#  
#  |    | geocodigo   |   anio | valor   |
#  |---:|:------------|-------:|:--------|
#  |  0 | AFG         |   1961 | NA      |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1965')
#  Index: 6504 entries, 4 to 6875
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   geocodigo  6504 non-null   object
#   1   anio       6504 non-null   int64 
#   2   valor      6504 non-null   object
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  4 | AFG         |   1965 |  0.9501 |
#  
#  ------------------------------
#  
#  drop_na(col=['valor'])
#  Index: 6504 entries, 4 to 6875
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   geocodigo  6504 non-null   object
#   1   anio       6504 non-null   int64 
#   2   valor      6504 non-null   object
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  4 | AFG         |   1965 |  0.9501 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 6504 entries, 0 to 6503
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   geocodigo  6504 non-null   object
#   1   anio       6504 non-null   int64 
#   2   valor      6504 non-null   object
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1965 |  0.9501 |
#  
#  ------------------------------
#  
#  query(condition="geocodigo != 'F351'")
#  Index: 6472 entries, 0 to 6503
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   geocodigo  6472 non-null   object
#   1   anio       6472 non-null   int64 
#   2   valor      6472 non-null   object
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1965 |  0.9501 |
#  
#  ------------------------------
#  