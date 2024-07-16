from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df

@transformer.convert
def str_to_title(df: DataFrame, col:str):
    df[col] = df[col].str.title()
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'exportaciones': 'indicador', 'provincia': 'geocodigo', 'fob': 'valor'}),
	replace_values(col='geocodigo', values={'catamarca': 'AR-K', 'jujuy': 'AR-Y', 'salta': 'AR-A', 'san_juan': 'AR-J', 'santa_cruz': 'AR-Z'}),
	str_to_title(col='indicador'),
	sort_values(how='ascending', by=['geocodigo', 'anio', 'indicador'])
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
#  rename_cols(map={'exportaciones': 'indicador', 'provincia': 'geocodigo', 'fob': 'valor'})
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       250 non-null    int64 
#   1   geocodigo  250 non-null    object
#   2   indicador  250 non-null    object
#   3   valor      250 non-null    int64 
#  
#  |    |   anio | geocodigo   | indicador   |     valor |
#  |---:|-------:|:------------|:------------|----------:|
#  |  0 |   1998 | catamarca   | mineras     | 438881324 |
#  
#  ------------------------------
#  
#  replace_values(col='geocodigo', values={'catamarca': 'AR-K', 'jujuy': 'AR-Y', 'salta': 'AR-A', 'san_juan': 'AR-J', 'santa_cruz': 'AR-Z'})
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       250 non-null    int64 
#   1   geocodigo  250 non-null    object
#   2   indicador  250 non-null    object
#   3   valor      250 non-null    int64 
#  
#  |    |   anio | geocodigo   | indicador   |     valor |
#  |---:|-------:|:------------|:------------|----------:|
#  |  0 |   1998 | AR-K        | Mineras     | 438881324 |
#  
#  ------------------------------
#  
#  str_to_title(col='indicador')
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       250 non-null    int64 
#   1   geocodigo  250 non-null    object
#   2   indicador  250 non-null    object
#   3   valor      250 non-null    int64 
#  
#  |    |   anio | geocodigo   | indicador   |     valor |
#  |---:|-------:|:------------|:------------|----------:|
#  |  0 |   1998 | AR-K        | Mineras     | 438881324 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['geocodigo', 'anio', 'indicador'])
#  RangeIndex: 250 entries, 0 to 249
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       250 non-null    int64 
#   1   geocodigo  250 non-null    object
#   2   indicador  250 non-null    object
#   3   valor      250 non-null    int64 
#  
#  |    |   anio | geocodigo   | indicador   |    valor |
#  |---:|-------:|:------------|:------------|---------:|
#  |  0 |   1998 | AR-A        | Mineras     | 35395200 |
#  
#  ------------------------------
#  