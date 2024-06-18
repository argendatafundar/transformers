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
#  RangeIndex: 9551 entries, 0 to 9550
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              9551 non-null   object 
#   1   iso3_desc_fundar  9551 non-null   object 
#   2   anio              9551 non-null   int64  
#   3   valor             9551 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | AFG    | Afganistán         |   1961 |  700000 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F15', new_value='BLX')
#  RangeIndex: 9551 entries, 0 to 9550
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              9551 non-null   object 
#   1   iso3_desc_fundar  9551 non-null   object 
#   2   anio              9551 non-null   int64  
#   3   valor             9551 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | AFG    | Afganistán         |   1961 |  700000 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F228', new_value='SVU')
#  RangeIndex: 9551 entries, 0 to 9550
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              9551 non-null   object 
#   1   iso3_desc_fundar  9551 non-null   object 
#   2   anio              9551 non-null   int64  
#   3   valor             9551 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | AFG    | Afganistán         |   1961 |  700000 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F248', new_value='SER')
#  RangeIndex: 9551 entries, 0 to 9550
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              9551 non-null   object 
#   1   iso3_desc_fundar  9551 non-null   object 
#   2   anio              9551 non-null   int64  
#   3   valor             9551 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | AFG    | Afganistán         |   1961 |  700000 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F51', new_value='CSK')
#  RangeIndex: 9551 entries, 0 to 9550
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              9551 non-null   object 
#   1   iso3_desc_fundar  9551 non-null   object 
#   2   anio              9551 non-null   int64  
#   3   valor             9551 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | AFG    | Afganistán         |   1961 |  700000 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 9551 entries, 0 to 9550
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         9551 non-null   object 
#   1   iso3_desc_fundar  9551 non-null   object 
#   2   anio              9551 non-null   int64  
#   3   valor             9551 non-null   float64
#  
#  |    | geocodigo   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:------------|:-------------------|-------:|--------:|
#  |  0 | AFG         | Afganistán         |   1961 |  700000 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc_fundar', axis=1)
#  RangeIndex: 9551 entries, 0 to 9550
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  9551 non-null   object 
#   1   anio       9551 non-null   int64  
#   2   valor      9551 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1961 |  700000 |
#  
#  ------------------------------
#  