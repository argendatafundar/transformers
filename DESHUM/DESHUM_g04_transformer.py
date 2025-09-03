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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'expectativa_educ': 'valor'}),
	drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1),
	apply_lambda(col='geonombreFundar', lambda_str='lambda x: x.replace("Países de desarrollo", "P. de desarrollo")', new_col=None)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 6798 entries, 0 to 6797
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6798 non-null   object 
#   1   geonombreFundar    6798 non-null   object 
#   2   continente_fundar  6435 non-null   object 
#   3   es_agregacion      6435 non-null   float64
#   4   anio               6798 non-null   int64  
#   5   expectativa_vida   6798 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   expectativa_vida |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|-------------------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 |              45.97 |
#  
#  ------------------------------
#  
#  rename_cols(map={'expectativa_educ': 'valor'})
#  RangeIndex: 6798 entries, 0 to 6797
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    6798 non-null   object 
#   1   geonombreFundar    6798 non-null   object 
#   2   continente_fundar  6435 non-null   object 
#   3   es_agregacion      6435 non-null   float64
#   4   anio               6798 non-null   int64  
#   5   expectativa_vida   6798 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   expectativa_vida |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|-------------------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 |              45.97 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'continente_fundar', 'es_agregacion'], axis=1)
#  RangeIndex: 6798 entries, 0 to 6797
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geonombreFundar   6798 non-null   object 
#   1   anio              6798 non-null   int64  
#   2   expectativa_vida  6798 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   expectativa_vida |
#  |---:|:------------------|-------:|-------------------:|
#  |  0 | Afganistán        |   1990 |              45.97 |
#  
#  ------------------------------
#  
#  apply_lambda(col='geonombreFundar', lambda_str='lambda x: x.replace("Países de desarrollo", "P. de desarrollo")', new_col=None)
#  RangeIndex: 6798 entries, 0 to 6797
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geonombreFundar   6798 non-null   object 
#   1   anio              6798 non-null   int64  
#   2   expectativa_vida  6798 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   expectativa_vida |
#  |---:|:------------------|-------:|-------------------:|
#  |  0 | Afganistán        |   1990 |              45.97 |
#  
#  ------------------------------
#  