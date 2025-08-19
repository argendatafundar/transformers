from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='anio == 2022'),
	rename_cols(map={'indice_va_trab': 'valor', 'letra_desc_abrev': 'categoria'}),
	drop_col(col=['letra', 'anio', 'va_por_trabajador'], axis=1),
	sort_mixed(sort_instructions={'valor': 'ascending'})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 128 entries, 0 to 127
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               128 non-null    int64  
#   1   letra              128 non-null    object 
#   2   letra_desc_abrev   128 non-null    object 
#   3   va_por_trabajador  128 non-null    float64
#   4   indice_va_trab     128 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   va_por_trabajador |   indice_va_trab |
#  |---:|-------:|:--------|:-------------------|--------------------:|-----------------:|
#  |  0 |   2016 | A       | Agro               |             370.038 |          106.623 |
#  
#  ------------------------------
#  
#  query(condition='anio == 2022')
#  Index: 16 entries, 6 to 126
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               16 non-null     int64  
#   1   letra              16 non-null     object 
#   2   letra_desc_abrev   16 non-null     object 
#   3   va_por_trabajador  16 non-null     float64
#   4   indice_va_trab     16 non-null     float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   va_por_trabajador |   indice_va_trab |
#  |---:|-------:|:--------|:-------------------|--------------------:|-----------------:|
#  |  6 |   2022 | A       | Agro               |              3839.1 |          121.375 |
#  
#  ------------------------------
#  
#  rename_cols(map={'indice_va_trab': 'valor', 'letra_desc_abrev': 'categoria'})
#  Index: 16 entries, 6 to 126
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               16 non-null     int64  
#   1   letra              16 non-null     object 
#   2   categoria          16 non-null     object 
#   3   va_por_trabajador  16 non-null     float64
#   4   valor              16 non-null     float64
#  
#  |    |   anio | letra   | categoria   |   va_por_trabajador |   valor |
#  |---:|-------:|:--------|:------------|--------------------:|--------:|
#  |  6 |   2022 | A       | Agro        |              3839.1 | 121.375 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra', 'anio', 'va_por_trabajador'], axis=1)
#  Index: 16 entries, 6 to 126
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  16 non-null     object 
#   1   valor      16 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  6 | Agro        | 121.375 |
#  
#  ------------------------------
#  
#  sort_mixed(sort_instructions={'valor': 'ascending'})
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  16 non-null     object 
#   1   valor      16 non-null     float64
#  
#  |    | categoria          |   valor |
#  |---:|:-------------------|--------:|
#  |  0 | Servicio doméstico | 8.68611 |
#  
#  ------------------------------
#  