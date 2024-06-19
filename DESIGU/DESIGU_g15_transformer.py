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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'ano': 'anio', 'variable': 'categoria'}),
	replace_value(col='categoria', curr_value='linea', new_value='Promedio'),
	replace_value(col='categoria', curr_value='participacion', new_value='Participación')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 172 entries, 0 to 171
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       172 non-null    int64  
#   1   variable  172 non-null    object 
#   2   valor     172 non-null    float64
#  
#  |    |   ano | variable      |   valor |
#  |---:|------:|:--------------|--------:|
#  |  0 |  1935 | participacion |    35.5 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'categoria'})
#  RangeIndex: 172 entries, 0 to 171
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       172 non-null    int64  
#   1   categoria  172 non-null    object 
#   2   valor      172 non-null    float64
#  
#  |    |   anio | categoria     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1935 | participacion |    35.5 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='linea', new_value='Promedio')
#  RangeIndex: 172 entries, 0 to 171
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       172 non-null    int64  
#   1   categoria  172 non-null    object 
#   2   valor      172 non-null    float64
#  
#  |    |   anio | categoria     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1935 | participacion |    35.5 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='participacion', new_value='Participación')
#  RangeIndex: 172 entries, 0 to 171
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       172 non-null    int64  
#   1   categoria  172 non-null    object 
#   2   valor      172 non-null    float64
#  
#  |    |   anio | categoria     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1935 | Participación |    35.5 |
#  
#  ------------------------------
#  