from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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
	query(condition='anio == 2019 & productividad_tipo == "Productividad por ocupado"'),
	rename_cols(map={'geocodigoFundar': 'geocodigo'}),
	drop_col(col=['productividad_tipo', 'anio'], axis=1),
	sort_mixed(sort_instructions={'valor': 'descending'})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 25620 entries, 0 to 25619
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     25620 non-null  object 
#   1   geonombreFundar     25620 non-null  object 
#   2   anio                25620 non-null  int64  
#   3   productividad_tipo  25620 non-null  object 
#   4   valor               13021 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | productividad_tipo     |   valor |
#  |---:|:------------------|:------------------|-------:|:-----------------------|--------:|
#  |  0 | ABW               | Aruba             |   1950 | Productividad por hora |     nan |
#  
#  ------------------------------
#  
#  query(condition='anio == 2019 & productividad_tipo == "Productividad por ocupado"')
#  Index: 183 entries, 139 to 25619
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     183 non-null    object 
#   1   geonombreFundar     183 non-null    object 
#   2   anio                183 non-null    int64  
#   3   productividad_tipo  183 non-null    object 
#   4   valor               177 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | productividad_tipo        |   valor |
#  |----:|:------------------|:------------------|-------:|:--------------------------|--------:|
#  | 139 | ABW               | Aruba             |   2019 | Productividad por ocupado | 64468.5 |
#  
#  ------------------------------
#  
#  rename_cols(map={'geocodigoFundar': 'geocodigo'})
#  Index: 183 entries, 139 to 25619
#  Data columns (total 5 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigo           183 non-null    object 
#   1   geonombreFundar     183 non-null    object 
#   2   anio                183 non-null    int64  
#   3   productividad_tipo  183 non-null    object 
#   4   valor               177 non-null    float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | productividad_tipo        |   valor |
#  |----:|:------------|:------------------|-------:|:--------------------------|--------:|
#  | 139 | ABW         | Aruba             |   2019 | Productividad por ocupado | 64468.5 |
#  
#  ------------------------------
#  
#  drop_col(col=['productividad_tipo', 'anio'], axis=1)
#  Index: 183 entries, 139 to 25619
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        183 non-null    object 
#   1   geonombreFundar  183 non-null    object 
#   2   valor            177 non-null    float64
#  
#  |     | geocodigo   | geonombreFundar   |   valor |
#  |----:|:------------|:------------------|--------:|
#  | 139 | ABW         | Aruba             | 64468.5 |
#  
#  ------------------------------
#  
#  sort_mixed(sort_instructions={'valor': 'descending'})
#  RangeIndex: 183 entries, 0 to 182
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        183 non-null    object 
#   1   geonombreFundar  183 non-null    object 
#   2   valor            177 non-null    float64
#  
#  |    | geocodigo   | geonombreFundar   |   valor |
#  |---:|:------------|:------------------|--------:|
#  |  0 | IRL         | Irlanda           |  209112 |
#  
#  ------------------------------
#  