from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'fuente': 'indicador', 'decil': 'categoria', 'proporcion': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100),
	sort_mixed(sort_instructions={'categoria': ['Decil 10', 'Decil 9', 'Decil 8', 'Decil 7', 'Decil 6', 'Decil 5', 'Decil 4', 'Decil 3', 'Decil 2', 'Decil 1'], 'indicador': ['Ingreso laboral', 'Ingreso de capital', 'Ingreso de jubilaciones', 'Ingreso de transferencias estatales', 'Otros ingresos']})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 50 entries, 0 to 49
#  Data columns (total 5 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   year        50 non-null     int64  
#   1   semestre    50 non-null     int64  
#   2   decil       50 non-null     object 
#   3   fuente      50 non-null     object 
#   4   proporcion  50 non-null     float64
#  
#  |    |   year |   semestre | decil   | fuente          |   proporcion |
#  |---:|-------:|-----------:|:--------|:----------------|-------------:|
#  |  0 |   2024 |          1 | Decil 1 | Ingreso laboral |     0.574017 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente': 'indicador', 'decil': 'categoria', 'proporcion': 'valor'})
#  RangeIndex: 50 entries, 0 to 49
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   year       50 non-null     int64   
#   1   semestre   50 non-null     int64   
#   2   categoria  50 non-null     category
#   3   indicador  50 non-null     category
#   4   valor      50 non-null     float64 
#  
#  |    |   year |   semestre | categoria   | indicador       |   valor |
#  |---:|-------:|-----------:|:------------|:----------------|--------:|
#  |  0 |   2024 |          1 | Decil 1     | Ingreso laboral | 57.4017 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 50 entries, 0 to 49
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   year       50 non-null     int64   
#   1   semestre   50 non-null     int64   
#   2   categoria  50 non-null     category
#   3   indicador  50 non-null     category
#   4   valor      50 non-null     float64 
#  
#  |    |   year |   semestre | categoria   | indicador       |   valor |
#  |---:|-------:|-----------:|:------------|:----------------|--------:|
#  |  0 |   2024 |          1 | Decil 1     | Ingreso laboral | 57.4017 |
#  
#  ------------------------------
#  
#  sort_mixed(sort_instructions={'categoria': ['Decil 10', 'Decil 9', 'Decil 8', 'Decil 7', 'Decil 6', 'Decil 5', 'Decil 4', 'Decil 3', 'Decil 2', 'Decil 1'], 'indicador': ['Ingreso laboral', 'Ingreso de capital', 'Ingreso de jubilaciones', 'Ingreso de transferencias estatales', 'Otros ingresos']})
#  RangeIndex: 50 entries, 0 to 49
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   year       50 non-null     int64   
#   1   semestre   50 non-null     int64   
#   2   categoria  50 non-null     category
#   3   indicador  50 non-null     category
#   4   valor      50 non-null     float64 
#  
#  |    |   year |   semestre | categoria   | indicador       |   valor |
#  |---:|-------:|-----------:|:------------|:----------------|--------:|
#  |  0 |   2024 |          1 | Decil 10    | Ingreso laboral | 81.4938 |
#  
#  ------------------------------
#  