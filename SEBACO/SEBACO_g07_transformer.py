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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='provincia', curr_value='CABA', new_value='AR-C'),
	replace_value(col='provincia', curr_value='Córdoba', new_value='AR-X'),
	replace_value(col='provincia', curr_value='GBA', new_value='AR-GBA'),
	replace_value(col='provincia', curr_value='Resto', new_value='SEBACO_AR-RESTO'),
	replace_value(col='provincia', curr_value='Resto de Buenos Aires', new_value='SEBACO_AR-B-RESTO'),
	replace_value(col='provincia', curr_value='Santa Fe', new_value='AR-S'),
	rename_cols(map={'provincia': 'geocodigo'}),
	rename_cols(map={'prop': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   prop       162 non-null    float64
#  
#  |    | provincia   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | CABA        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='CABA', new_value='AR-C')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   prop       162 non-null    float64
#  
#  |    | provincia   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | AR-C        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Córdoba', new_value='AR-X')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   prop       162 non-null    float64
#  
#  |    | provincia   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | AR-C        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='GBA', new_value='AR-GBA')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   prop       162 non-null    float64
#  
#  |    | provincia   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | AR-C        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Resto', new_value='SEBACO_AR-RESTO')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   prop       162 non-null    float64
#  
#  |    | provincia   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | AR-C        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Resto de Buenos Aires', new_value='SEBACO_AR-B-RESTO')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   prop       162 non-null    float64
#  
#  |    | provincia   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | AR-C        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  replace_value(col='provincia', curr_value='Santa Fe', new_value='AR-S')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   prop       162 non-null    float64
#  
#  |    | provincia   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | AR-C        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia': 'geocodigo'})
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   prop       162 non-null    float64
#  
#  |    | geocodigo   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | AR-C        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop': 'valor'})
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AR-C        |   1996 | 41.0583 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AR-C        |   1996 | 41.0583 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  162 non-null    object 
#   1   anio       162 non-null    int64  
#   2   valor      162 non-null    float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AR-C        |   1996 | 41.0583 |
#  
#  ------------------------------
#  