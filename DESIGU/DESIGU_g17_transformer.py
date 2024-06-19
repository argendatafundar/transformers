from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
rename_cols(map={'ano': 'anio', 'variable': 'geocodigo'}),
	replace_value(col='geocodigo', curr_value='argentina', new_value='ARG'),
	replace_value(col='geocodigo', curr_value='gba', new_value='AR-GBA'),
	replace_value(col='geocodigo', curr_value='pampeana', new_value='AR-PAM'),
	replace_value(col='geocodigo', curr_value='cuyo', new_value='AR-CUY'),
	replace_value(col='geocodigo', curr_value='nea', new_value='AR-NEA'),
	replace_value(col='geocodigo', curr_value='noa', new_value='AR-NOA'),
	replace_value(col='geocodigo', curr_value='patagonia', new_value='AR-PAT')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 119 entries, 0 to 118
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       119 non-null    int64  
#   1   variable  119 non-null    object 
#   2   valor     119 non-null    float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  2006 | argentina  |    45.5 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'geocodigo'})
#  RangeIndex: 119 entries, 0 to 118
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       119 non-null    int64  
#   1   geocodigo  119 non-null    object 
#   2   valor      119 non-null    float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2006 | argentina   |    45.5 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='argentina', new_value='ARG')
#  RangeIndex: 119 entries, 0 to 118
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       119 non-null    int64  
#   1   geocodigo  119 non-null    object 
#   2   valor      119 non-null    float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2006 | ARG         |    45.5 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='gba', new_value='AR-GBA')
#  RangeIndex: 119 entries, 0 to 118
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       119 non-null    int64  
#   1   geocodigo  119 non-null    object 
#   2   valor      119 non-null    float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2006 | ARG         |    45.5 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='pampeana', new_value='AR-PAM')
#  RangeIndex: 119 entries, 0 to 118
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       119 non-null    int64  
#   1   geocodigo  119 non-null    object 
#   2   valor      119 non-null    float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2006 | ARG         |    45.5 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='cuyo', new_value='AR-CUY')
#  RangeIndex: 119 entries, 0 to 118
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       119 non-null    int64  
#   1   geocodigo  119 non-null    object 
#   2   valor      119 non-null    float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2006 | ARG         |    45.5 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='nea', new_value='AR-NEA')
#  RangeIndex: 119 entries, 0 to 118
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       119 non-null    int64  
#   1   geocodigo  119 non-null    object 
#   2   valor      119 non-null    float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2006 | ARG         |    45.5 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='noa', new_value='AR-NOA')
#  RangeIndex: 119 entries, 0 to 118
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       119 non-null    int64  
#   1   geocodigo  119 non-null    object 
#   2   valor      119 non-null    float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2006 | ARG         |    45.5 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='patagonia', new_value='AR-PAT')
#  RangeIndex: 119 entries, 0 to 118
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       119 non-null    int64  
#   1   geocodigo  119 non-null    object 
#   2   valor      119 non-null    float64
#  
#  |    |   anio | geocodigo   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2006 | ARG         |    45.5 |
#  
#  ------------------------------
#  