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

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='F15', new_value='BLX'),
	replace_value(col='iso3', curr_value='F228', new_value='SVU'),
	replace_value(col='iso3', curr_value='F248', new_value='SER'),
	replace_value(col='iso3', curr_value='F51', new_value='CSK'),
	rename_cols(map={'iso3': 'geocodigo'}),
	drop_col(col='iso3_desc_fundar', axis=1),
	sort_values(how='ascending', by=['anio', 'geocodigo']),
	query(condition="geocodigo != 'F351'"),
	multiplicar_por_escalar(col='valor', k=1e-06)
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
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
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
#  query(condition="geocodigo != 'F351'")
#  Index: 9489 entries, 0 to 9550
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  9489 non-null   object 
#   1   anio       9489 non-null   int64  
#   2   valor      9489 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1961 |     0.7 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  Index: 9489 entries, 0 to 9550
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  9489 non-null   object 
#   1   anio       9489 non-null   int64  
#   2   valor      9489 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AFG         |   1961 |     0.7 |
#  
#  ------------------------------
#  