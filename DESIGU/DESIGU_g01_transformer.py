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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='variable', curr_value='EPH', new_value='Datos EPH'),
	replace_value(col='variable', curr_value='ajustados', new_value='Datos ajustados'),
	replace_value(col='decil', curr_value=1, new_value='Decil 1'),
	replace_value(col='decil', curr_value=2, new_value='Decil 2'),
	replace_value(col='decil', curr_value=3, new_value='Decil 3'),
	replace_value(col='decil', curr_value=4, new_value='Decil 4'),
	replace_value(col='decil', curr_value=5, new_value='Decil 5'),
	replace_value(col='decil', curr_value=6, new_value='Decil 6'),
	replace_value(col='decil', curr_value=7, new_value='Decil 7'),
	replace_value(col='decil', curr_value=8, new_value='Decil 8'),
	replace_value(col='decil', curr_value=9, new_value='Decil 9'),
	replace_value(col='decil', curr_value=10, new_value='Decil 10'),
	rename_cols(map={'decil': 'categoria', 'variable': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     int64  
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    |   decil | variable   |   valor |
#  |---:|--------:|:-----------|--------:|
#  |  0 |       1 | EPH        |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='EPH', new_value='Datos EPH')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     int64  
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    |   decil | variable   |   valor |
#  |---:|--------:|:-----------|--------:|
#  |  0 |       1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='ajustados', new_value='Datos ajustados')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     int64  
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    |   decil | variable   |   valor |
#  |---:|--------:|:-----------|--------:|
#  |  0 |       1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='decil', curr_value=1, new_value='Decil 1')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     object 
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    | decil   | variable   |   valor |
#  |---:|:--------|:-----------|--------:|
#  |  0 | Decil 1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='decil', curr_value=2, new_value='Decil 2')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     object 
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    | decil   | variable   |   valor |
#  |---:|:--------|:-----------|--------:|
#  |  0 | Decil 1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='decil', curr_value=3, new_value='Decil 3')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     object 
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    | decil   | variable   |   valor |
#  |---:|:--------|:-----------|--------:|
#  |  0 | Decil 1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='decil', curr_value=4, new_value='Decil 4')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     object 
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    | decil   | variable   |   valor |
#  |---:|:--------|:-----------|--------:|
#  |  0 | Decil 1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='decil', curr_value=5, new_value='Decil 5')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     object 
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    | decil   | variable   |   valor |
#  |---:|:--------|:-----------|--------:|
#  |  0 | Decil 1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='decil', curr_value=6, new_value='Decil 6')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     object 
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    | decil   | variable   |   valor |
#  |---:|:--------|:-----------|--------:|
#  |  0 | Decil 1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='decil', curr_value=7, new_value='Decil 7')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     object 
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    | decil   | variable   |   valor |
#  |---:|:--------|:-----------|--------:|
#  |  0 | Decil 1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='decil', curr_value=8, new_value='Decil 8')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     object 
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    | decil   | variable   |   valor |
#  |---:|:--------|:-----------|--------:|
#  |  0 | Decil 1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='decil', curr_value=9, new_value='Decil 9')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     object 
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    | decil   | variable   |   valor |
#  |---:|:--------|:-----------|--------:|
#  |  0 | Decil 1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  replace_value(col='decil', curr_value=10, new_value='Decil 10')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   decil     20 non-null     object 
#   1   variable  20 non-null     object 
#   2   valor     20 non-null     float64
#  
#  |    | decil   | variable   |   valor |
#  |---:|:--------|:-----------|--------:|
#  |  0 | Decil 1 | Datos EPH  |   1.904 |
#  
#  ------------------------------
#  
#  rename_cols(map={'decil': 'categoria', 'variable': 'indicador'})
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  20 non-null     object 
#   1   indicador  20 non-null     object 
#   2   valor      20 non-null     float64
#  
#  |    | categoria   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | Decil 1     | Datos EPH   |   1.904 |
#  
#  ------------------------------
#  