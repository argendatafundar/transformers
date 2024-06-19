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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'ano': 'anio', 'variable': 'categoria'}),
	replace_value(col='categoria', curr_value='mujeresbaja', new_value='Mujeres con calificación baja'),
	replace_value(col='categoria', curr_value='mujeresmedia', new_value='Mujeres con calificación media'),
	replace_value(col='categoria', curr_value='mujeresalta', new_value='Mujeres con calificación alta'),
	replace_value(col='categoria', curr_value='hombres', new_value='Hombres'),
	replace_value(col='categoria', curr_value='mujeres', new_value='Mujeres')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       105 non-null    int64  
#   1   variable  105 non-null    object 
#   2   valor     105 non-null    float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  2003 | mujeres    |    67.2 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'categoria'})
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       105 non-null    int64  
#   1   categoria  105 non-null    object 
#   2   valor      105 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2003 | mujeres     |    67.2 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='mujeresbaja', new_value='Mujeres con calificación baja')
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       105 non-null    int64  
#   1   categoria  105 non-null    object 
#   2   valor      105 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2003 | mujeres     |    67.2 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='mujeresmedia', new_value='Mujeres con calificación media')
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       105 non-null    int64  
#   1   categoria  105 non-null    object 
#   2   valor      105 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2003 | mujeres     |    67.2 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='mujeresalta', new_value='Mujeres con calificación alta')
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       105 non-null    int64  
#   1   categoria  105 non-null    object 
#   2   valor      105 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2003 | mujeres     |    67.2 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='hombres', new_value='Hombres')
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       105 non-null    int64  
#   1   categoria  105 non-null    object 
#   2   valor      105 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2003 | mujeres     |    67.2 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='mujeres', new_value='Mujeres')
#  RangeIndex: 105 entries, 0 to 104
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       105 non-null    int64  
#   1   categoria  105 non-null    object 
#   2   valor      105 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2003 | Mujeres     |    67.2 |
#  
#  ------------------------------
#  