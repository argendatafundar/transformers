from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

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
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="variable.isin(['hombres1','mujeres1'])"),
	replace_value(col='variable', curr_value='hombres1', new_value='Varones', mapping=None),
	replace_value(col='variable', curr_value='mujeres1', new_value='Mujeres', mapping=None),
	rename_cols(map={'variable': 'instancia'}),
	pivot_longer(id_cols=['instancia'], names_to_col='variable', values_to_col='value')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 204 entries, 0 to 203
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   edad      204 non-null    int64  
#   1   variable  204 non-null    object 
#   2   valor     204 non-null    float64
#  
#  |    |   edad | variable   |   valor |
#  |---:|-------:|:-----------|--------:|
#  |  0 |     25 | hombres1   |  360178 |
#  
#  ------------------------------
#  
#  query(condition="variable.isin(['hombres1','mujeres1'])")
#  Index: 102 entries, 0 to 201
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   edad      102 non-null    int64  
#   1   variable  102 non-null    object 
#   2   valor     102 non-null    float64
#  
#  |    |   edad | variable   |   valor |
#  |---:|-------:|:-----------|--------:|
#  |  0 |     25 | hombres1   |  360178 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='hombres1', new_value='Varones', mapping=None)
#  Index: 102 entries, 0 to 201
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   edad      102 non-null    int64  
#   1   variable  102 non-null    object 
#   2   valor     102 non-null    float64
#  
#  |    |   edad | variable   |   valor |
#  |---:|-------:|:-----------|--------:|
#  |  0 |     25 | Varones    |  360178 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='mujeres1', new_value='Mujeres', mapping=None)
#  Index: 102 entries, 0 to 201
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   edad      102 non-null    int64  
#   1   variable  102 non-null    object 
#   2   valor     102 non-null    float64
#  
#  |    |   edad | variable   |   valor |
#  |---:|-------:|:-----------|--------:|
#  |  0 |     25 | Varones    |  360178 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'instancia'})
#  Index: 102 entries, 0 to 201
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   edad       102 non-null    int64  
#   1   instancia  102 non-null    object 
#   2   valor      102 non-null    float64
#  
#  |    |   edad | instancia   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |     25 | Varones     |  360178 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['instancia'], names_to_col='variable', values_to_col='value')
#  RangeIndex: 204 entries, 0 to 203
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   instancia  204 non-null    object 
#   1   variable   204 non-null    object 
#   2   value      204 non-null    float64
#  
#  |    | instancia   | variable   |   value |
#  |---:|:------------|:-----------|--------:|
#  |  0 | Varones     | edad       |      25 |
#  
#  ------------------------------
#  