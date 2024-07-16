from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='CHA', new_value='CHI'),
	query(condition='anio == anio.max()'),
	drop_col(col='iso3_desc_fundar', axis=1),
	drop_col(col='anio', axis=1),
	rename_cols(map={'iso3': 'geocodigo'}),
	query(condition='geocodigo != "F351" ')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 224 entries, 0 to 223
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              224 non-null    object 
#   1   iso3_desc_fundar  224 non-null    object 
#   2   anio              224 non-null    int64  
#   3   valor             224 non-null    float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | ABW    | Aruba              |   2021 |       2 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='CHA', new_value='CHI')
#  RangeIndex: 224 entries, 0 to 223
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              224 non-null    object 
#   1   iso3_desc_fundar  224 non-null    object 
#   2   anio              224 non-null    int64  
#   3   valor             224 non-null    float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | ABW    | Aruba              |   2021 |       2 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 224 entries, 0 to 223
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              224 non-null    object 
#   1   iso3_desc_fundar  224 non-null    object 
#   2   anio              224 non-null    int64  
#   3   valor             224 non-null    float64
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   valor |
#  |---:|:-------|:-------------------|-------:|--------:|
#  |  0 | ABW    | Aruba              |   2021 |       2 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc_fundar', axis=1)
#  Index: 224 entries, 0 to 223
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   iso3    224 non-null    object 
#   1   anio    224 non-null    int64  
#   2   valor   224 non-null    float64
#  
#  |    | iso3   |   anio |   valor |
#  |---:|:-------|-------:|--------:|
#  |  0 | ABW    |   2021 |       2 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 224 entries, 0 to 223
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   iso3    224 non-null    object 
#   1   valor   224 non-null    float64
#  
#  |    | iso3   |   valor |
#  |---:|:-------|--------:|
#  |  0 | ABW    |       2 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo'})
#  Index: 224 entries, 0 to 223
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  224 non-null    object 
#   1   valor      224 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | ABW         |       2 |
#  
#  ------------------------------
#  
#  query(condition='geocodigo != "F351" ')
#  Index: 223 entries, 0 to 223
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  223 non-null    object 
#   1   valor      223 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | ABW         |       2 |
#  
#  ------------------------------
#  