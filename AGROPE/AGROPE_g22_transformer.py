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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='F15', new_value='BLX'),
	replace_value(col='iso3', curr_value='F228', new_value='SVU'),
	replace_value(col='iso3', curr_value='F248', new_value='SER'),
	replace_value(col='iso3', curr_value='F51', new_value='CSK'),
	rename_cols(map={'iso3': 'geocodigo'}),
	drop_col(col='iso3_desc_fundar', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 11393 entries, 0 to 11392
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              11393 non-null  int64  
#   1   iso3              11393 non-null  object 
#   2   iso3_desc_fundar  11393 non-null  object 
#   3   valor             11393 non-null  float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar   |   valor |
#  |---:|-------:|:-------|:-------------------|--------:|
#  |  0 |   1961 | AFG    | Afganistán         |  360000 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F15', new_value='BLX')
#  RangeIndex: 11393 entries, 0 to 11392
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              11393 non-null  int64  
#   1   iso3              11393 non-null  object 
#   2   iso3_desc_fundar  11393 non-null  object 
#   3   valor             11393 non-null  float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar   |   valor |
#  |---:|-------:|:-------|:-------------------|--------:|
#  |  0 |   1961 | AFG    | Afganistán         |  360000 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F228', new_value='SVU')
#  RangeIndex: 11393 entries, 0 to 11392
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              11393 non-null  int64  
#   1   iso3              11393 non-null  object 
#   2   iso3_desc_fundar  11393 non-null  object 
#   3   valor             11393 non-null  float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar   |   valor |
#  |---:|-------:|:-------|:-------------------|--------:|
#  |  0 |   1961 | AFG    | Afganistán         |  360000 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F248', new_value='SER')
#  RangeIndex: 11393 entries, 0 to 11392
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              11393 non-null  int64  
#   1   iso3              11393 non-null  object 
#   2   iso3_desc_fundar  11393 non-null  object 
#   3   valor             11393 non-null  float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar   |   valor |
#  |---:|-------:|:-------|:-------------------|--------:|
#  |  0 |   1961 | AFG    | Afganistán         |  360000 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F51', new_value='CSK')
#  RangeIndex: 11393 entries, 0 to 11392
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              11393 non-null  int64  
#   1   iso3              11393 non-null  object 
#   2   iso3_desc_fundar  11393 non-null  object 
#   3   valor             11393 non-null  float64
#  
#  |    |   anio | iso3   | iso3_desc_fundar   |   valor |
#  |---:|-------:|:-------|:-------------------|--------:|
#  |  0 |   1961 | AFG    | Afganistán         |  360000 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 11393 entries, 0 to 11392
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              11393 non-null  int64  
#   1   geocodigo         11393 non-null  object 
#   2   iso3_desc_fundar  11393 non-null  object 
#   3   valor             11393 non-null  float64
#  
#  |    |   anio | geocodigo   | iso3_desc_fundar   |   valor |
#  |---:|-------:|:------------|:-------------------|--------:|
#  |  0 |   1961 | AFG         | Afganistán         |  360000 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc_fundar', axis=1)
#  RangeIndex: 11393 entries, 0 to 11392
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       11393 non-null  int64  
#   1   geocodigo  11393 non-null  object 
#   2   valor      11393 non-null  float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1961 | AFG         |  360000 |
#  
#  ------------------------------
#  