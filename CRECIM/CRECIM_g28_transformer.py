from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
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
drop_col(col='pib_per_capita', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100),
	replace_value(col='geocodigo', curr_value='WRL_MPD', new_value='WLD'),
	replace_value(col='geocodigo', curr_value='YUG', new_value='SER'),
	replace_value(col='geocodigo', curr_value='SUN', new_value='SVU'),
	sort_values(how='ascending', by=['anio', 'geocodigo']),
	query(condition='anio >= 1900 & anio <= 1975')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 5995 entries, 0 to 5994
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             5995 non-null   int64  
#   1   iso3             5995 non-null   object 
#   2   pib_per_capita   5995 non-null   float64
#   3   cambio_relativo  5995 non-null   float64
#  
#  |    |   anio | iso3   |   pib_per_capita |   cambio_relativo |
#  |---:|-------:|:-------|-----------------:|------------------:|
#  |  0 |   1900 | ALB    |             1092 |                 0 |
#  
#  ------------------------------
#  
#  drop_col(col='pib_per_capita', axis=1)
#  RangeIndex: 5995 entries, 0 to 5994
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             5995 non-null   int64  
#   1   iso3             5995 non-null   object 
#   2   cambio_relativo  5995 non-null   float64
#  
#  |    |   anio | iso3   |   cambio_relativo |
#  |---:|-------:|:-------|------------------:|
#  |  0 |   1900 | ALB    |                 0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'cambio_relativo': 'valor'})
#  RangeIndex: 5995 entries, 0 to 5994
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       5995 non-null   int64  
#   1   geocodigo  5995 non-null   object 
#   2   valor      5995 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1900 | ALB         |       0 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 5995 entries, 0 to 5994
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       5995 non-null   int64  
#   1   geocodigo  5995 non-null   object 
#   2   valor      5995 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1900 | ALB         |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='WRL_MPD', new_value='WLD')
#  RangeIndex: 5995 entries, 0 to 5994
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       5995 non-null   int64  
#   1   geocodigo  5995 non-null   object 
#   2   valor      5995 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1900 | ALB         |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='YUG', new_value='SER')
#  RangeIndex: 5995 entries, 0 to 5994
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       5995 non-null   int64  
#   1   geocodigo  5995 non-null   object 
#   2   valor      5995 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1900 | ALB         |       0 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='SUN', new_value='SVU')
#  RangeIndex: 5995 entries, 0 to 5994
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       5995 non-null   int64  
#   1   geocodigo  5995 non-null   object 
#   2   valor      5995 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1900 | ALB         |       0 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 5995 entries, 0 to 5994
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       5995 non-null   int64  
#   1   geocodigo  5995 non-null   object 
#   2   valor      5995 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1900 | ALB         |       0 |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1900 & anio <= 1975')
#  Index: 3561 entries, 0 to 3560
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3561 non-null   int64  
#   1   geocodigo  3561 non-null   object 
#   2   valor      3561 non-null   float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1900 | ALB         |       0 |
#  
#  ------------------------------
#  