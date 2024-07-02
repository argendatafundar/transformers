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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='F15', new_value='BLX'),
	replace_value(col='iso3', curr_value='F228', new_value='SVU'),
	replace_value(col='iso3', curr_value='F248', new_value='SER'),
	replace_value(col='iso3', curr_value='F51', new_value='CSK'),
	rename_cols(map={'iso3': 'geocodigo', 'rindes_maiz_ma5': 'valor'}),
	drop_col(col='iso3_desc_fundar', axis=1),
	drop_col(col='rindes', axis=1),
	query(condition='anio >= 1965')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 9403 entries, 0 to 9402
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              9403 non-null   object 
#   1   iso3_desc_fundar  9403 non-null   object 
#   2   anio              9403 non-null   int64  
#   3   rindes            9403 non-null   float64
#   4   rindes_maiz_ma5   9403 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_maiz_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:------------------|
#  |  0 | AFG    | Afganistán         |   1961 |      1.4 | NA                |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F15', new_value='BLX')
#  RangeIndex: 9403 entries, 0 to 9402
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              9403 non-null   object 
#   1   iso3_desc_fundar  9403 non-null   object 
#   2   anio              9403 non-null   int64  
#   3   rindes            9403 non-null   float64
#   4   rindes_maiz_ma5   9403 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_maiz_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:------------------|
#  |  0 | AFG    | Afganistán         |   1961 |      1.4 | NA                |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F228', new_value='SVU')
#  RangeIndex: 9403 entries, 0 to 9402
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              9403 non-null   object 
#   1   iso3_desc_fundar  9403 non-null   object 
#   2   anio              9403 non-null   int64  
#   3   rindes            9403 non-null   float64
#   4   rindes_maiz_ma5   9403 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_maiz_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:------------------|
#  |  0 | AFG    | Afganistán         |   1961 |      1.4 | NA                |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F248', new_value='SER')
#  RangeIndex: 9403 entries, 0 to 9402
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              9403 non-null   object 
#   1   iso3_desc_fundar  9403 non-null   object 
#   2   anio              9403 non-null   int64  
#   3   rindes            9403 non-null   float64
#   4   rindes_maiz_ma5   9403 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_maiz_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:------------------|
#  |  0 | AFG    | Afganistán         |   1961 |      1.4 | NA                |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F51', new_value='CSK')
#  RangeIndex: 9403 entries, 0 to 9402
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              9403 non-null   object 
#   1   iso3_desc_fundar  9403 non-null   object 
#   2   anio              9403 non-null   int64  
#   3   rindes            9403 non-null   float64
#   4   rindes_maiz_ma5   9403 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_maiz_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:------------------|
#  |  0 | AFG    | Afganistán         |   1961 |      1.4 | NA                |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'rindes_maiz_ma5': 'valor'})
#  RangeIndex: 9403 entries, 0 to 9402
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         9403 non-null   object 
#   1   iso3_desc_fundar  9403 non-null   object 
#   2   anio              9403 non-null   int64  
#   3   rindes            9403 non-null   float64
#   4   valor             9403 non-null   object 
#  
#  |    | geocodigo   | iso3_desc_fundar   |   anio |   rindes | valor   |
#  |---:|:------------|:-------------------|-------:|---------:|:--------|
#  |  0 | AFG         | Afganistán         |   1961 |      1.4 | NA      |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc_fundar', axis=1)
#  RangeIndex: 9403 entries, 0 to 9402
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  9403 non-null   object 
#   1   anio       9403 non-null   int64  
#   2   rindes     9403 non-null   float64
#   3   valor      9403 non-null   object 
#  
#  |    | geocodigo   |   anio |   rindes | valor   |
#  |---:|:------------|-------:|---------:|:--------|
#  |  0 | AFG         |   1961 |      1.4 | NA      |
#  
#  ------------------------------
#  
#  drop_col(col='rindes', axis=1)
#  RangeIndex: 9403 entries, 0 to 9402
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   geocodigo  9403 non-null   object
#   1   anio       9403 non-null   int64 
#   2   valor      9403 non-null   object
#  
#  |    | geocodigo   |   anio | valor   |
#  |---:|:------------|-------:|:--------|
#  |  0 | AFG         |   1961 | NA      |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1965')
#  Index: 8847 entries, 4 to 9402
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   geocodigo  8847 non-null   object
#   1   anio       8847 non-null   int64 
#   2   valor      8847 non-null   object
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  4 | AFG         |   1965 | 1.41834 |
#  
#  ------------------------------
#  