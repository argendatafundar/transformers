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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'variable': 'categoria', 'ano': 'anio'}),
	replace_value(col='categoria', curr_value='(15-24)', new_value='15 a 24 años'),
	replace_value(col='categoria', curr_value='(25-64)', new_value='25 a 64 años'),
	replace_value(col='categoria', curr_value='(65-)', new_value='65 años y más')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 160 entries, 0 to 159
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype 
#  ---  ------    --------------  ----- 
#   0   ano       160 non-null    int64 
#   1   variable  160 non-null    object
#   2   valor     160 non-null    int64 
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1992 | Mujeres    |   11178 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'categoria', 'ano': 'anio'})
#  RangeIndex: 160 entries, 0 to 159
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       160 non-null    int64 
#   1   categoria  160 non-null    object
#   2   valor      160 non-null    int64 
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Mujeres     |   11178 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='(15-24)', new_value='15 a 24 años')
#  RangeIndex: 160 entries, 0 to 159
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       160 non-null    int64 
#   1   categoria  160 non-null    object
#   2   valor      160 non-null    int64 
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Mujeres     |   11178 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='(25-64)', new_value='25 a 64 años')
#  RangeIndex: 160 entries, 0 to 159
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       160 non-null    int64 
#   1   categoria  160 non-null    object
#   2   valor      160 non-null    int64 
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Mujeres     |   11178 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='(65-)', new_value='65 años y más')
#  RangeIndex: 160 entries, 0 to 159
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       160 non-null    int64 
#   1   categoria  160 non-null    object
#   2   valor      160 non-null    int64 
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Mujeres     |   11178 |
#  
#  ------------------------------
#  