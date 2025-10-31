from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'variable': 'categoria', 'ano': 'anio'}),
	replace_value(col='categoria', curr_value='(15-24)', new_value='15 a 24 años', mapping=None),
	replace_value(col='categoria', curr_value='(25-64)', new_value='25 a 64 años', mapping=None),
	replace_value(col='categoria', curr_value='(65 +)', new_value='65 años y más', mapping=None),
	replace_value(col='categoria', curr_value='Hombres', new_value='Varones', mapping=None)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 165 entries, 0 to 164
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       165 non-null    int64  
#   1   variable  165 non-null    object 
#   2   valor     165 non-null    float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1992 | Mujeres    | 11177.6 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'categoria', 'ano': 'anio'})
#  RangeIndex: 165 entries, 0 to 164
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       165 non-null    int64  
#   1   categoria  165 non-null    object 
#   2   valor      165 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Mujeres     | 11177.6 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='(15-24)', new_value='15 a 24 años', mapping=None)
#  RangeIndex: 165 entries, 0 to 164
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       165 non-null    int64  
#   1   categoria  165 non-null    object 
#   2   valor      165 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Mujeres     | 11177.6 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='(25-64)', new_value='25 a 64 años', mapping=None)
#  RangeIndex: 165 entries, 0 to 164
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       165 non-null    int64  
#   1   categoria  165 non-null    object 
#   2   valor      165 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Mujeres     | 11177.6 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='(65 +)', new_value='65 años y más', mapping=None)
#  RangeIndex: 165 entries, 0 to 164
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       165 non-null    int64  
#   1   categoria  165 non-null    object 
#   2   valor      165 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Mujeres     | 11177.6 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Hombres', new_value='Varones', mapping=None)
#  RangeIndex: 165 entries, 0 to 164
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       165 non-null    int64  
#   1   categoria  165 non-null    object 
#   2   valor      165 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Mujeres     | 11177.6 |
#  
#  ------------------------------
#  