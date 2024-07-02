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
	rename_cols(map={'iso3': 'geocodigo', 'rindes_soja_ma5': 'valor'}),
	drop_col(col='iso3_desc_fundar', axis=1),
	drop_col(col='rindes', axis=1),
	query(condition='anio >= 1965')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4771 entries, 0 to 4770
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4771 non-null   object 
#   1   iso3_desc_fundar  4771 non-null   object 
#   2   anio              4771 non-null   int64  
#   3   rindes            4771 non-null   float64
#   4   rindes_soja_ma5   4771 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_soja_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:------------------|
#  |  0 | AGO    | Angola             |   2000 |   0.2333 | NA                |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F15', new_value='BLX')
#  RangeIndex: 4771 entries, 0 to 4770
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4771 non-null   object 
#   1   iso3_desc_fundar  4771 non-null   object 
#   2   anio              4771 non-null   int64  
#   3   rindes            4771 non-null   float64
#   4   rindes_soja_ma5   4771 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_soja_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:------------------|
#  |  0 | AGO    | Angola             |   2000 |   0.2333 | NA                |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F228', new_value='SVU')
#  RangeIndex: 4771 entries, 0 to 4770
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4771 non-null   object 
#   1   iso3_desc_fundar  4771 non-null   object 
#   2   anio              4771 non-null   int64  
#   3   rindes            4771 non-null   float64
#   4   rindes_soja_ma5   4771 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_soja_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:------------------|
#  |  0 | AGO    | Angola             |   2000 |   0.2333 | NA                |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F248', new_value='SER')
#  RangeIndex: 4771 entries, 0 to 4770
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4771 non-null   object 
#   1   iso3_desc_fundar  4771 non-null   object 
#   2   anio              4771 non-null   int64  
#   3   rindes            4771 non-null   float64
#   4   rindes_soja_ma5   4771 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_soja_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:------------------|
#  |  0 | AGO    | Angola             |   2000 |   0.2333 | NA                |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='F51', new_value='CSK')
#  RangeIndex: 4771 entries, 0 to 4770
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              4771 non-null   object 
#   1   iso3_desc_fundar  4771 non-null   object 
#   2   anio              4771 non-null   int64  
#   3   rindes            4771 non-null   float64
#   4   rindes_soja_ma5   4771 non-null   object 
#  
#  |    | iso3   | iso3_desc_fundar   |   anio |   rindes | rindes_soja_ma5   |
#  |---:|:-------|:-------------------|-------:|---------:|:------------------|
#  |  0 | AGO    | Angola             |   2000 |   0.2333 | NA                |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'rindes_soja_ma5': 'valor'})
#  RangeIndex: 4771 entries, 0 to 4770
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         4771 non-null   object 
#   1   iso3_desc_fundar  4771 non-null   object 
#   2   anio              4771 non-null   int64  
#   3   rindes            4771 non-null   float64
#   4   valor             4771 non-null   object 
#  
#  |    | geocodigo   | iso3_desc_fundar   |   anio |   rindes | valor   |
#  |---:|:------------|:-------------------|-------:|---------:|:--------|
#  |  0 | AGO         | Angola             |   2000 |   0.2333 | NA      |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc_fundar', axis=1)
#  RangeIndex: 4771 entries, 0 to 4770
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  4771 non-null   object 
#   1   anio       4771 non-null   int64  
#   2   rindes     4771 non-null   float64
#   3   valor      4771 non-null   object 
#  
#  |    | geocodigo   |   anio |   rindes | valor   |
#  |---:|:------------|-------:|---------:|:--------|
#  |  0 | AGO         |   2000 |   0.2333 | NA      |
#  
#  ------------------------------
#  
#  drop_col(col='rindes', axis=1)
#  RangeIndex: 4771 entries, 0 to 4770
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   geocodigo  4771 non-null   object
#   1   anio       4771 non-null   int64 
#   2   valor      4771 non-null   object
#  
#  |    | geocodigo   |   anio | valor   |
#  |---:|:------------|-------:|:--------|
#  |  0 | AGO         |   2000 | NA      |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1965')
#  Index: 4576 entries, 0 to 4770
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   geocodigo  4576 non-null   object
#   1   anio       4576 non-null   int64 
#   2   valor      4576 non-null   object
#  
#  |    | geocodigo   |   anio | valor   |
#  |---:|:------------|-------:|:--------|
#  |  0 | AGO         |   2000 | NA      |
#  
#  ------------------------------
#  