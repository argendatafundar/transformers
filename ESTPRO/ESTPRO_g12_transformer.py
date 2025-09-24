from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def sort_mixed(
    df, 
    sort_instructions: dict
):
    """
    Sorts a DataFrame by multiple columns, supporting both categorical (with custom order) and numerical (with direction) sorting.

    Args:
        df: Input DataFrame.
        sort_instructions: Dictionary where keys are column names and values are either:
            - a list of categories (for categorical columns, sorted in that order)
            - a string 'ascending' or 'descending' (for numerical or string columns)

    Returns:
        DataFrame sorted by the specified columns in the given order/direction.
    """
    import pandas as pd

    by = []
    ascending = []

    for col, instruction in sort_instructions.items():
        if isinstance(instruction, list):
            # Categorical sort
            df[col] = pd.Categorical(df[col], categories=instruction, ordered=True)
            by.append(col)
            ascending.append(True)
        elif instruction in ['ascending', 'descending']:
            by.append(col)
            ascending.append(instruction == 'ascending')
        else:
            raise ValueError(f"Invalid sort instruction for column '{col}': {instruction}")

    return df.sort_values(by=by, ascending=ascending).reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'letra_desc_abrev': 'categoria', 'porc_mujeres': 'valor'}),
	drop_col(col=['letra'], axis=1),
	multiplicar_por_escalar(col='valor', k=100),
	query(condition='anio in [1996, 2008, 2022]'),
	sort_mixed(sort_instructions={'categoria': 'ascending', 'anio': 'ascending'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 435 entries, 0 to 434
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              435 non-null    int64  
#   1   letra             435 non-null    object 
#   2   letra_desc_abrev  435 non-null    object 
#   3   porc_mujeres      435 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   porc_mujeres |
#  |---:|-------:|:--------|:-------------------|---------------:|
#  |  0 |   1996 | A       | Agro               |      0.0921219 |
#  
#  ------------------------------
#  
#  rename_cols(map={'letra_desc_abrev': 'categoria', 'porc_mujeres': 'valor'})
#  RangeIndex: 435 entries, 0 to 434
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       435 non-null    int64  
#   1   letra      435 non-null    object 
#   2   categoria  435 non-null    object 
#   3   valor      435 non-null    float64
#  
#  |    |   anio | letra   | categoria   |     valor |
#  |---:|-------:|:--------|:------------|----------:|
#  |  0 |   1996 | A       | Agro        | 0.0921219 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra'], axis=1)
#  RangeIndex: 435 entries, 0 to 434
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       435 non-null    int64  
#   1   categoria  435 non-null    object 
#   2   valor      435 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1996 | Agro        | 9.21219 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 435 entries, 0 to 434
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       435 non-null    int64  
#   1   categoria  435 non-null    object 
#   2   valor      435 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1996 | Agro        | 9.21219 |
#  
#  ------------------------------
#  
#  query(condition='anio in [1996, 2008, 2022]')
#  Index: 45 entries, 0 to 404
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       45 non-null     int64  
#   1   categoria  45 non-null     object 
#   2   valor      45 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1996 | Agro        | 9.21219 |
#  
#  ------------------------------
#  
#  sort_mixed(sort_instructions={'categoria': 'ascending', 'anio': 'ascending'})
#  RangeIndex: 45 entries, 0 to 44
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       45 non-null     int64  
#   1   categoria  45 non-null     object 
#   2   valor      45 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1996 | Agro        | 9.21219 |
#  
#  ------------------------------
#  