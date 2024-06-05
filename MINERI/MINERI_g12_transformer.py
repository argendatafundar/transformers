from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
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
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys=['anio', 'provincia', 'exportaciones'], value_name='valor', var_name='indicador'),
	rename_cols(map={'provincia': 'geoselector', 'exportaciones': 'serie'}),
	replace_value(col='geoselector', curr_value='catamarca', new_value='AR-Z'),
	replace_value(col='geoselector', curr_value='jujuy', new_value='AR-Y'),
	replace_value(col='geoselector', curr_value='salta', new_value='AR-A'),
	replace_value(col='geoselector', curr_value='san juan', new_value='AR-J'),
	replace_value(col='geoselector', curr_value='santa cruz', new_value='AR-Z')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype 
#  ---  ------         --------------  ----- 
#   0   anio           250 non-null    int64 
#   1   provincia      250 non-null    object
#   2   exportaciones  250 non-null    object
#   3   fob            250 non-null    int64 
#  
#  |    |   anio | provincia   | exportaciones   |       fob |
#  |---:|-------:|:------------|:----------------|----------:|
#  |  0 |   1998 | catamarca   | mineras         | 438881324 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'provincia', 'exportaciones'], value_name='valor', var_name='indicador')
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 5 columns):
#   #   Column         Non-Null Count  Dtype 
#  ---  ------         --------------  ----- 
#   0   anio           250 non-null    int64 
#   1   provincia      250 non-null    object
#   2   exportaciones  250 non-null    object
#   3   indicador      250 non-null    object
#   4   valor          250 non-null    int64 
#  
#  |    |   anio | provincia   | exportaciones   | indicador   |     valor |
#  |---:|-------:|:------------|:----------------|:------------|----------:|
#  |  0 |   1998 | catamarca   | mineras         | fob         | 438881324 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia': 'geoselector', 'exportaciones': 'serie'})
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype 
#  ---  ------       --------------  ----- 
#   0   anio         250 non-null    int64 
#   1   geoselector  250 non-null    object
#   2   serie        250 non-null    object
#   3   indicador    250 non-null    object
#   4   valor        250 non-null    int64 
#  
#  |    |   anio | geoselector   | serie   | indicador   |     valor |
#  |---:|-------:|:--------------|:--------|:------------|----------:|
#  |  0 |   1998 | catamarca     | mineras | fob         | 438881324 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='catamarca', new_value='AR-Z')
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype 
#  ---  ------       --------------  ----- 
#   0   anio         250 non-null    int64 
#   1   geoselector  250 non-null    object
#   2   serie        250 non-null    object
#   3   indicador    250 non-null    object
#   4   valor        250 non-null    int64 
#  
#  |    |   anio | geoselector   | serie   | indicador   |     valor |
#  |---:|-------:|:--------------|:--------|:------------|----------:|
#  |  0 |   1998 | AR-Z          | mineras | fob         | 438881324 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='jujuy', new_value='AR-Y')
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype 
#  ---  ------       --------------  ----- 
#   0   anio         250 non-null    int64 
#   1   geoselector  250 non-null    object
#   2   serie        250 non-null    object
#   3   indicador    250 non-null    object
#   4   valor        250 non-null    int64 
#  
#  |    |   anio | geoselector   | serie   | indicador   |     valor |
#  |---:|-------:|:--------------|:--------|:------------|----------:|
#  |  0 |   1998 | AR-Z          | mineras | fob         | 438881324 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='salta', new_value='AR-A')
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype 
#  ---  ------       --------------  ----- 
#   0   anio         250 non-null    int64 
#   1   geoselector  250 non-null    object
#   2   serie        250 non-null    object
#   3   indicador    250 non-null    object
#   4   valor        250 non-null    int64 
#  
#  |    |   anio | geoselector   | serie   | indicador   |     valor |
#  |---:|-------:|:--------------|:--------|:------------|----------:|
#  |  0 |   1998 | AR-Z          | mineras | fob         | 438881324 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='san juan', new_value='AR-J')
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype 
#  ---  ------       --------------  ----- 
#   0   anio         250 non-null    int64 
#   1   geoselector  250 non-null    object
#   2   serie        250 non-null    object
#   3   indicador    250 non-null    object
#   4   valor        250 non-null    int64 
#  
#  |    |   anio | geoselector   | serie   | indicador   |     valor |
#  |---:|-------:|:--------------|:--------|:------------|----------:|
#  |  0 |   1998 | AR-Z          | mineras | fob         | 438881324 |
#  
#  ------------------------------
#  
#  replace_value(col='geoselector', curr_value='santa cruz', new_value='AR-Z')
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 5 columns):
#   #   Column       Non-Null Count  Dtype 
#  ---  ------       --------------  ----- 
#   0   anio         250 non-null    int64 
#   1   geoselector  250 non-null    object
#   2   serie        250 non-null    object
#   3   indicador    250 non-null    object
#   4   valor        250 non-null    int64 
#  
#  |    |   anio | geoselector   | serie   | indicador   |     valor |
#  |---:|-------:|:--------------|:--------|:------------|----------:|
#  |  0 |   1998 | AR-Z          | mineras | fob         | 438881324 |
#  
#  ------------------------------
#  