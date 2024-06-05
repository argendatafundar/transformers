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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='provincia', curr_value='CABA', new_value='AR-C'),
	replace_value(col='provincia', curr_value='Córdoba', new_value='AR-X'),
	replace_value(col='provincia', curr_value='GBA', new_value='AR-GBA'),
	replace_value(col='provincia', curr_value='Resto', new_value='AR-RESTO-SEBACO'),
	replace_value(col='provincia', curr_value='Resto de Buenos Aires', new_value='AR-B-RESTO-SEBACO'),
	replace_value(col='provincia', curr_value='Santa Fe', new_value='AR-S'),
	rename_cols(map={'provincia': 'geocodigo'}),
	rename_cols(map={'prop': 'valor'})
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
#  replace_value(col='provincia', curr_value='Resto', new_value='AR-RESTO-SEBACO')
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
#  replace_value(col='provincia', curr_value='Resto de Buenos Aires', new_value='AR-B-RESTO-SEBACO')
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
#  |    | geocodigo   |   anio |    valor |
#  |---:|:------------|-------:|---------:|
#  |  0 | AR-C        |   1996 | 0.410583 |
#  
#  ------------------------------
#  