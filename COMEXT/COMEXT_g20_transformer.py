from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
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
	query(condition="geocodigoFundar == 'ARG'"),
	drop_col(col='reportingeconomy', axis=1),
	rename_cols(map={'productsector_agregado': 'indicador', 'export_value_pc': 'valor', 'year': 'anio'}),
	replace_value(col=None, curr_value=None, new_value=None, mapping={'indicador': {'Servicios de telecomunicaciones, informática e información': 'Serv. de telecom., informática e información'}}),
	sort_mixed(sort_instructions={'indicador': ['Viajes', 'Otros servicios empresariales', 'Serv. de telecom., informática e información', 'Transportes', 'Resto']})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 16314 entries, 0 to 16313
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         16314 non-null  object 
#   1   geonombreFundar         16314 non-null  object 
#   2   year                    16314 non-null  int64  
#   3   reportingeconomy        16314 non-null  object 
#   4   productsector_agregado  16314 non-null  object 
#   5   export_value_pc         16312 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | reportingeconomy                       | productsector_agregado        |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|:---------------------------------------|:------------------------------|------------------:|
#  |  0 | ABW               | Aruba             |   2005 | Aruba, the Netherlands with respect to | Otros servicios empresariales |           9.33435 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar == 'ARG'")
#  Index: 90 entries, 23 to 15652
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         90 non-null     object 
#   1   geonombreFundar         90 non-null     object 
#   2   year                    90 non-null     int64  
#   3   reportingeconomy        90 non-null     object 
#   4   productsector_agregado  90 non-null     object 
#   5   export_value_pc         90 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | reportingeconomy   | productsector_agregado        |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|:-------------------|:------------------------------|------------------:|
#  | 23 | ARG               | Argentina         |   2005 | Argentina          | Otros servicios empresariales |           24.7445 |
#  
#  ------------------------------
#  
#  drop_col(col='reportingeconomy', axis=1)
#  Index: 90 entries, 23 to 15652
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         90 non-null     object 
#   1   geonombreFundar         90 non-null     object 
#   2   year                    90 non-null     int64  
#   3   productsector_agregado  90 non-null     object 
#   4   export_value_pc         90 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | productsector_agregado        |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|:------------------------------|------------------:|
#  | 23 | ARG               | Argentina         |   2005 | Otros servicios empresariales |           24.7445 |
#  
#  ------------------------------
#  
#  rename_cols(map={'productsector_agregado': 'indicador', 'export_value_pc': 'valor', 'year': 'anio'})
#  Index: 90 entries, 23 to 15652
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  90 non-null     object 
#   1   geonombreFundar  90 non-null     object 
#   2   anio             90 non-null     int64  
#   3   indicador        90 non-null     object 
#   4   valor            90 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | indicador                     |   valor |
#  |---:|:------------------|:------------------|-------:|:------------------------------|--------:|
#  | 23 | ARG               | Argentina         |   2005 | Otros servicios empresariales | 24.7445 |
#  
#  ------------------------------
#  
#  replace_value(col=None, curr_value=None, new_value=None, mapping={'indicador': {'Servicios de telecomunicaciones, informática e información': 'Serv. de telecom., informática e información'}})
#  Index: 90 entries, 23 to 15652
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  90 non-null     object  
#   1   geonombreFundar  90 non-null     object  
#   2   anio             90 non-null     int64   
#   3   indicador        90 non-null     category
#   4   valor            90 non-null     float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | indicador                     |   valor |
#  |---:|:------------------|:------------------|-------:|:------------------------------|--------:|
#  | 23 | ARG               | Argentina         |   2005 | Otros servicios empresariales | 24.7445 |
#  
#  ------------------------------
#  
#  sort_mixed(sort_instructions={'indicador': ['Viajes', 'Otros servicios empresariales', 'Serv. de telecom., informática e información', 'Transportes', 'Resto']})
#  RangeIndex: 90 entries, 0 to 89
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  90 non-null     object  
#   1   geonombreFundar  90 non-null     object  
#   2   anio             90 non-null     int64   
#   3   indicador        90 non-null     category
#   4   valor            90 non-null     float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | indicador   |   valor |
#  |---:|:------------------|:------------------|-------:|:------------|--------:|
#  |  0 | ARG               | Argentina         |   2013 | Viajes      | 33.0775 |
#  
#  ------------------------------
#  