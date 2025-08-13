from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_na(df, subset:str): 
    return df.dropna(subset=subset, axis=0)

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
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def calcular_ranking(
    df, 
    index: list, 
    query = None,
    groupby = None, 
    rank_col: str = 'ranking', 
    value_col: str = 'valor', 
    method: str = 'dense',
    agg: str = 'sum'
):

    if query is not None and not isinstance(query, str):
        raise ValueError("query must be a string or None")

    # INSERT_YOUR_CODE
    if groupby is not None and not isinstance(groupby, list):
        raise ValueError("groupby must be a list or None")

    df_copy = df.copy()
    if query is not None:
        df_copy = df_copy.query(query)

    # If groupby is specified, aggregate value_col before ranking
    if groupby is not None:
        # Aggregate value_col
        df_copy = df_copy.groupby(groupby, as_index=False)[value_col].agg(agg)
        # Compute ranking
        df_copy[rank_col] = df_copy[value_col].rank(method=method, ascending=False)
        # Merge ranking back to original df on groupby columns
        df = df.merge(df_copy[groupby + [rank_col]], on=groupby, how='left')
    else:
        # Compute ranking without aggregation
        df_copy[rank_col] = df_copy[value_col].rank(method=method, ascending=False)
        df = df.merge(df_copy[index + [rank_col]], on=index, how='left')
    return df

@transformer.convert
def expand_and_fill(df: pd.DataFrame, agrupacion: list, index: dict, objetivo: dict) -> pd.DataFrame:
    """
    Expands DataFrame to include all possible combinations of index values for each group
    and fills missing values in target columns with default values.

    Args:
        df: Input DataFrame
        agrupacion: List of column names to group by
        index: Dictionary mapping column names to lists of possible values
        objetivo: Dictionary mapping target column names to default fill values

    Returns:
        DataFrame with expanded combinations and filled values
    """
    # Validate columns
    for col in agrupacion:
        if col not in df.columns:
            raise ValueError(f"Grouping column '{col}' not found in DataFrame")
    for col in index.keys():
        if col not in df.columns:
            raise ValueError(f"Index column '{col}' not found in DataFrame")
    for col in objetivo.keys():
        if col not in df.columns:
            raise ValueError(f"Target column '{col}' not found in DataFrame")

    # Build MultiIndex for expansion
    group_vals = [df[col].unique() for col in agrupacion]
    index_vals = [index[col] for col in index]
    import pandas as pd

    # Create a MultiIndex with all combinations
    import itertools
    all_combinations = list(itertools.product(*group_vals, *index_vals))
    all_columns = agrupacion + list(index.keys())
    expanded_index = pd.MultiIndex.from_tuples(all_combinations, names=all_columns)
    df_expanded = df.set_index(all_columns).reindex(expanded_index).reset_index()

    # Fill missing values in target columns
    for col, default in objetivo.items():
        df_expanded[col] = df_expanded[col].fillna(default)

    return df_expanded

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	latest_year(by='anio'),
	rename_cols(map={'calificacion': 'indicador', 'letra_desc_abrev': 'categoria', 'particip_calif': 'valor'}),
	drop_col(col=['letra', 'calificacion_cod'], axis=1),
	multiplicar_por_escalar(col='valor', k=100),
	drop_na(subset='valor'),
	replace_value(col='categoria', curr_value='Organizaciones extraterritoriales', new_value='Org. extraterritoriales'),
	replace_value(col='categoria', curr_value='Serv. comunitarios, sociales y personales', new_value='Serv. comun., soc. y pers.'),
	replace_value(col='categoria', curr_value='Información y comunicaciones', new_value='Info. y comunicaciones'),
	expand_and_fill(agrupacion=['categoria'], index={'indicador': ['Calificado', 'No calificado', 'Semicalificado']}, objetivo={'valor': 0}),
	calcular_ranking(index=['categoria'], query='indicador == "Calificado"', groupby=None, rank_col='ranking', value_col='valor', method='dense', agg='sum'),
	sort_mixed(sort_instructions={'ranking': 'descending', 'indicador': ['Calificado', 'Semicalificado', 'No calificado']})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 514 entries, 0 to 513
#  Data columns (total 6 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              514 non-null    int64  
#   1   letra             514 non-null    object 
#   2   letra_desc_abrev  514 non-null    object 
#   3   calificacion_cod  514 non-null    int64  
#   4   calificacion      514 non-null    object 
#   5   particip_calif    514 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev            |   calificacion_cod | calificacion   |   particip_calif |
#  |---:|-------:|:--------|:----------------------------|-------------------:|:---------------|-----------------:|
#  |  0 |   2016 | N       | Actividades administrativas |                  1 | Calificado     |         0.113746 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 65 entries, 7 to 513
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   letra             65 non-null     object 
#   1   letra_desc_abrev  65 non-null     object 
#   2   calificacion_cod  65 non-null     int64  
#   3   calificacion      65 non-null     object 
#   4   particip_calif    65 non-null     float64
#  
#  |    | letra   | letra_desc_abrev            |   calificacion_cod | calificacion   |   particip_calif |
#  |---:|:--------|:----------------------------|-------------------:|:---------------|-----------------:|
#  |  7 | N       | Actividades administrativas |                  1 | Calificado     |         0.104882 |
#  
#  ------------------------------
#  
#  rename_cols(map={'calificacion': 'indicador', 'letra_desc_abrev': 'categoria', 'particip_calif': 'valor'})
#  Index: 65 entries, 7 to 513
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   letra             65 non-null     object 
#   1   categoria         65 non-null     object 
#   2   calificacion_cod  65 non-null     int64  
#   3   indicador         65 non-null     object 
#   4   valor             65 non-null     float64
#  
#  |    | letra   | categoria                   |   calificacion_cod | indicador   |    valor |
#  |---:|:--------|:----------------------------|-------------------:|:------------|---------:|
#  |  7 | N       | Actividades administrativas |                  1 | Calificado  | 0.104882 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra', 'calificacion_cod'], axis=1)
#  Index: 65 entries, 7 to 513
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  65 non-null     object 
#   1   indicador  65 non-null     object 
#   2   valor      65 non-null     float64
#  
#  |    | categoria                   | indicador   |   valor |
#  |---:|:----------------------------|:------------|--------:|
#  |  7 | Actividades administrativas | Calificado  | 10.4882 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 65 entries, 7 to 513
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  65 non-null     object 
#   1   indicador  65 non-null     object 
#   2   valor      65 non-null     float64
#  
#  |    | categoria                   | indicador   |   valor |
#  |---:|:----------------------------|:------------|--------:|
#  |  7 | Actividades administrativas | Calificado  | 10.4882 |
#  
#  ------------------------------
#  
#  drop_na(subset='valor')
#  Index: 65 entries, 7 to 513
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  65 non-null     object 
#   1   indicador  65 non-null     object 
#   2   valor      65 non-null     float64
#  
#  |    | categoria                   | indicador   |   valor |
#  |---:|:----------------------------|:------------|--------:|
#  |  7 | Actividades administrativas | Calificado  | 10.4882 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Organizaciones extraterritoriales', new_value='Org. extraterritoriales')
#  Index: 65 entries, 7 to 513
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  65 non-null     object 
#   1   indicador  65 non-null     object 
#   2   valor      65 non-null     float64
#  
#  |    | categoria                   | indicador   |   valor |
#  |---:|:----------------------------|:------------|--------:|
#  |  7 | Actividades administrativas | Calificado  | 10.4882 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Serv. comunitarios, sociales y personales', new_value='Serv. comun., soc. y pers.')
#  Index: 65 entries, 7 to 513
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  65 non-null     object 
#   1   indicador  65 non-null     object 
#   2   valor      65 non-null     float64
#  
#  |    | categoria                   | indicador   |   valor |
#  |---:|:----------------------------|:------------|--------:|
#  |  7 | Actividades administrativas | Calificado  | 10.4882 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Información y comunicaciones', new_value='Info. y comunicaciones')
#  Index: 65 entries, 7 to 513
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  65 non-null     object 
#   1   indicador  65 non-null     object 
#   2   valor      65 non-null     float64
#  
#  |    | categoria                   | indicador   |   valor |
#  |---:|:----------------------------|:------------|--------:|
#  |  7 | Actividades administrativas | Calificado  | 10.4882 |
#  
#  ------------------------------
#  
#  expand_and_fill(agrupacion=['categoria'], index={'indicador': ['Calificado', 'No calificado', 'Semicalificado']}, objetivo={'valor': 0})
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  66 non-null     object 
#   1   indicador  66 non-null     object 
#   2   valor      66 non-null     float64
#  
#  |    | categoria                   | indicador   |   valor |
#  |---:|:----------------------------|:------------|--------:|
#  |  0 | Actividades administrativas | Calificado  | 10.4882 |
#  
#  ------------------------------
#  
#  calcular_ranking(index=['categoria'], query='indicador == "Calificado"', groupby=None, rank_col='ranking', value_col='valor', method='dense', agg='sum')
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  66 non-null     object  
#   1   indicador  66 non-null     category
#   2   valor      66 non-null     float64 
#   3   ranking    66 non-null     float64 
#  
#  |    | categoria                   | indicador   |   valor |   ranking |
#  |---:|:----------------------------|:------------|--------:|----------:|
#  |  0 | Actividades administrativas | Calificado  | 10.4882 |        18 |
#  
#  ------------------------------
#  
#  sort_mixed(sort_instructions={'ranking': 'descending', 'indicador': ['Calificado', 'Semicalificado', 'No calificado']})
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  66 non-null     object  
#   1   indicador  66 non-null     category
#   2   valor      66 non-null     float64 
#   3   ranking    66 non-null     float64 
#  
#  |    | categoria          | indicador   |    valor |   ranking |
#  |---:|:-------------------|:------------|---------:|----------:|
#  |  0 | Servicio doméstico | Calificado  | 0.774139 |        22 |
#  
#  ------------------------------
#  