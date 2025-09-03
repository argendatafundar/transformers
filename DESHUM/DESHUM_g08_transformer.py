from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def apply_lambda(df: DataFrame, col: str, lambda_str: str, new_col: str = None):
    """
    Apply a lambda function (passed as string) to a column in the DataFrame.

    Args:
        df: Input DataFrame
        col: Column name to apply the lambda function to
        lambda_str: Lambda function as string (e.g., "lambda x: x * 2")
        new_col: New column name for the result. If None, overwrites the original column

    Returns:
        DataFrame with the lambda function applied to the specified column

    Example:
        apply_lambda(df, 'price', 'lambda x: x * 1.1', 'price_with_tax')
        apply_lambda(df, 'name', 'lambda x: x.upper()')
    """
    import pandas as pd
    import numpy as np

    # Validate column exists
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in DataFrame")

    # Validate lambda string
    if not isinstance(lambda_str, str):
        raise ValueError("lambda_str must be a string")

    if not lambda_str.strip().startswith('lambda'):
        raise ValueError("lambda_str must start with 'lambda'")

    # Safe evaluation with restricted globals
    safe_globals = {
        '__builtins__': {},
        'pd': pd,
        'np': np,
        'pandas': pd,
        'numpy': np,
    }

    try:
        # Evaluate the lambda function
        lambda_func = eval(lambda_str, safe_globals)
    except Exception as e:
        raise ValueError(f"Invalid lambda expression: {e}")

    # Apply the lambda function to the column
    try:
        result = df[col].apply(lambda_func)
    except Exception as e:
        raise ValueError(f"Error applying lambda function to column '{col}': {e}")

    # Determine output column name
    output_col = new_col if new_col is not None else col

    # Create a copy to avoid modifying the original
    df_result = df.copy()
    df_result[output_col] = result

    return df_result

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='geonombreFundar == "Argentina"'),
	rename_cols(map={'idh_tipo': 'categoria'}),
	apply_lambda(col='categoria', lambda_str='lambda x: x.replace("IDH ", "")', new_col=None)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 40788 entries, 0 to 40787
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  40590 non-null  object 
#   1   geonombreFundar  40590 non-null  object 
#   2   country          40788 non-null  object 
#   3   anio             40788 non-null  int64  
#   4   idh_tipo         40788 non-null  object 
#   5   valor            38647 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | country     |   anio | idh_tipo                             |    valor |
#  |---:|:------------------|:------------------|:------------|-------:|:-------------------------------------|---------:|
#  |  0 | AFG               | Afganistán        | Afghanistan |   1990 | IDH años esperados de escolarización | 0.163137 |
#  
#  ------------------------------
#  
#  query(condition='geonombreFundar == "Argentina"')
#  Index: 198 entries, 990 to 1187
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  198 non-null    object 
#   1   geonombreFundar  198 non-null    object 
#   2   country          198 non-null    object 
#   3   anio             198 non-null    int64  
#   4   idh_tipo         198 non-null    object 
#   5   valor            198 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   | country   |   anio | idh_tipo                             |    valor |
#  |----:|:------------------|:------------------|:----------|-------:|:-------------------------------------|---------:|
#  | 990 | ARG               | Argentina         | Argentina |   1990 | IDH años esperados de escolarización | 0.744027 |
#  
#  ------------------------------
#  
#  rename_cols(map={'idh_tipo': 'categoria'})
#  Index: 198 entries, 990 to 1187
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  198 non-null    object 
#   1   geonombreFundar  198 non-null    object 
#   2   country          198 non-null    object 
#   3   anio             198 non-null    int64  
#   4   categoria        198 non-null    object 
#   5   valor            198 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   | country   |   anio | categoria                            |    valor |
#  |----:|:------------------|:------------------|:----------|-------:|:-------------------------------------|---------:|
#  | 990 | ARG               | Argentina         | Argentina |   1990 | IDH años esperados de escolarización | 0.744027 |
#  
#  ------------------------------
#  
#  apply_lambda(col='categoria', lambda_str='lambda x: x.replace("IDH ", "")', new_col=None)
#  Index: 198 entries, 990 to 1187
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  198 non-null    object 
#   1   geonombreFundar  198 non-null    object 
#   2   country          198 non-null    object 
#   3   anio             198 non-null    int64  
#   4   categoria        198 non-null    object 
#   5   valor            198 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   | country   |   anio | categoria                        |    valor |
#  |----:|:------------------|:------------------|:----------|-------:|:---------------------------------|---------:|
#  | 990 | ARG               | Argentina         | Argentina |   1990 | años esperados de escolarización | 0.744027 |
#  
#  ------------------------------
#  