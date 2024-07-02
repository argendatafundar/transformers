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
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='F15', new_value='BLX'),
	replace_value(col='iso3', curr_value='F228', new_value='SVU'),
	replace_value(col='iso3', curr_value='F248', new_value='SER'),
	replace_value(col='iso3', curr_value='F51', new_value='CSK'),
	rename_cols(map={'iso3': 'geocodigo'}),
	drop_col(col='iso3_desc_fundar', axis=1),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6138 entries, 0 to 6137
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              6138 non-null   object 
#   1   iso3_desc_fundar  6138 non-null   object 
#   2   anio              6138 non-null   int64  
#   3   valor             6138 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | AGO    | Angola             |   1961 |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F15', new_value='BLX')
#  RangeIndex: 6138 entries, 0 to 6137
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              6138 non-null   object 
#   1   iso3_desc_fundar  6138 non-null   object 
#   2   anio              6138 non-null   int64  
#   3   valor             6138 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | AGO    | Angola             |   1961 |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F228', new_value='SVU')
#  RangeIndex: 6138 entries, 0 to 6137
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              6138 non-null   object 
#   1   iso3_desc_fundar  6138 non-null   object 
#   2   anio              6138 non-null   int64  
#   3   valor             6138 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | AGO    | Angola             |   1961 |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F248', new_value='SER')
#  RangeIndex: 6138 entries, 0 to 6137
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              6138 non-null   object 
#   1   iso3_desc_fundar  6138 non-null   object 
#   2   anio              6138 non-null   int64  
#   3   valor             6138 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | AGO    | Angola             |   1961 |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F51', new_value='CSK')
#  RangeIndex: 6138 entries, 0 to 6137
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              6138 non-null   object 
#   1   iso3_desc_fundar  6138 non-null   object 
#   2   anio              6138 non-null   int64  
#   3   valor             6138 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | AGO    | Angola             |   1961 |       0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 6138 entries, 0 to 6137
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         6138 non-null   object 
#   1   iso3_desc_fundar  6138 non-null   object 
#   2   anio              6138 non-null   int64  
#   3   valor             6138 non-null   float64
#  
#  |    | geocodigo   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:------------|:-------------------|-------:|--------:|
#  |  0 | AGO         | Angola             |   1961 |       0 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc_fundar', axis=1)
#  RangeIndex: 6138 entries, 0 to 6137
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  6138 non-null   object 
#   1   anio       6138 non-null   int64  
#   2   valor      6138 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AGO         |   1961 |       0 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 6138 entries, 0 to 6137
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  6138 non-null   object 
#   1   anio       6138 non-null   int64  
#   2   valor      6138 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AGO         |   1961 |       0 |
#  
#  ------------------------------
#  