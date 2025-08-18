from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

@transformer.convert
def calcular_ranking(
    df: DataFrame, 
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
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	latest_year(by='anio'),
	rename_cols(map={'letra_desc_abrev': 'categoria'}),
	drop_col(col=['letra'], axis=1),
	wide_to_long(primary_keys=['categoria'], value_name='valor', var_name='indicador'),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='indicador', curr_value=None, new_value=None, mapping={'porc_mujeres': ' Mujeres', 'porc_varones': ' Varones'}),
	calcular_ranking(index=['categoria'], query="indicador == ' Mujeres'", groupby=None, rank_col='ranking', value_col='valor', method='dense', agg='sum'),
	sort_mixed(sort_instructions={'ranking': 'ascending', 'indicador': [' Mujeres', ' Varones']}),
	replace_value(col=None, curr_value=None, new_value=None, mapping={'categoria': {'Transporte y comunicaciones': 'Transp. y comunicaciones', 'Industria manufacturera': 'Ind. manufacturera', 'Servicio doméstico': 'Serv. doméstico', 'Serv. comunitarios, sociales y personales': 'Serv. com., soc y pers.', 'Serv. inmobiliarios y profesionales': 'Serv. inmob. y prof.'}})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 112 entries, 0 to 111
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              112 non-null    int64  
#   1   letra             112 non-null    object 
#   2   letra_desc_abrev  112 non-null    object 
#   3   porc_mujeres      112 non-null    float64
#   4   porc_varones      112 non-null    float64
#  
#  |    |   anio | letra   | letra_desc_abrev   |   porc_mujeres |   porc_varones |
#  |---:|-------:|:--------|:-------------------|---------------:|---------------:|
#  |  0 |   2016 | AB      | Agro y pesca       |       0.142606 |       0.857394 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 16 entries, 6 to 111
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   letra             16 non-null     object 
#   1   letra_desc_abrev  16 non-null     object 
#   2   porc_mujeres      16 non-null     float64
#   3   porc_varones      16 non-null     float64
#  
#  |    | letra   | letra_desc_abrev   |   porc_mujeres |   porc_varones |
#  |---:|:--------|:-------------------|---------------:|---------------:|
#  |  6 | AB      | Agro y pesca       |       0.140203 |       0.859797 |
#  
#  ------------------------------
#  
#  rename_cols(map={'letra_desc_abrev': 'categoria'})
#  Index: 16 entries, 6 to 111
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   letra         16 non-null     object 
#   1   categoria     16 non-null     object 
#   2   porc_mujeres  16 non-null     float64
#   3   porc_varones  16 non-null     float64
#  
#  |    | letra   | categoria    |   porc_mujeres |   porc_varones |
#  |---:|:--------|:-------------|---------------:|---------------:|
#  |  6 | AB      | Agro y pesca |       0.140203 |       0.859797 |
#  
#  ------------------------------
#  
#  drop_col(col=['letra'], axis=1)
#  Index: 16 entries, 6 to 111
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   categoria     16 non-null     object 
#   1   porc_mujeres  16 non-null     float64
#   2   porc_varones  16 non-null     float64
#  
#  |    | categoria    |   porc_mujeres |   porc_varones |
#  |---:|:-------------|---------------:|---------------:|
#  |  6 | Agro y pesca |       0.140203 |       0.859797 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['categoria'], value_name='valor', var_name='indicador')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   indicador  32 non-null     object 
#   2   valor      32 non-null     float64
#  
#  |    | categoria    | indicador    |   valor |
#  |---:|:-------------|:-------------|--------:|
#  |  0 | Agro y pesca | porc_mujeres | 14.0203 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   indicador  32 non-null     object 
#   2   valor      32 non-null     float64
#  
#  |    | categoria    | indicador    |   valor |
#  |---:|:-------------|:-------------|--------:|
#  |  0 | Agro y pesca | porc_mujeres | 14.0203 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value=None, new_value=None, mapping={'porc_mujeres': ' Mujeres', 'porc_varones': ' Varones'})
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   indicador  32 non-null     object 
#   2   valor      32 non-null     float64
#  
#  |    | categoria    | indicador   |   valor |
#  |---:|:-------------|:------------|--------:|
#  |  0 | Agro y pesca | Mujeres     | 14.0203 |
#  
#  ------------------------------
#  
#  calcular_ranking(index=['categoria'], query="indicador == ' Mujeres'", groupby=None, rank_col='ranking', value_col='valor', method='dense', agg='sum')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  32 non-null     object  
#   1   indicador  32 non-null     category
#   2   valor      32 non-null     float64 
#   3   ranking    32 non-null     float64 
#  
#  |    | categoria    | indicador   |   valor |   ranking |
#  |---:|:-------------|:------------|--------:|----------:|
#  |  0 | Agro y pesca | Mujeres     | 14.0203 |        14 |
#  
#  ------------------------------
#  
#  sort_mixed(sort_instructions={'ranking': 'ascending', 'indicador': [' Mujeres', ' Varones']})
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  32 non-null     object  
#   1   indicador  32 non-null     category
#   2   valor      32 non-null     float64 
#   3   ranking    32 non-null     float64 
#  
#  |    | categoria          | indicador   |   valor |   ranking |
#  |---:|:-------------------|:------------|--------:|----------:|
#  |  0 | Servicio doméstico | Mujeres     | 97.2356 |         1 |
#  
#  ------------------------------
#  
#  replace_value(col=None, curr_value=None, new_value=None, mapping={'categoria': {'Transporte y comunicaciones': 'Transp. y comunicaciones', 'Industria manufacturera': 'Ind. manufacturera', 'Servicio doméstico': 'Serv. doméstico', 'Serv. comunitarios, sociales y personales': 'Serv. com., soc y pers.', 'Serv. inmobiliarios y profesionales': 'Serv. inmob. y prof.'}})
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  32 non-null     object  
#   1   indicador  32 non-null     category
#   2   valor      32 non-null     float64 
#   3   ranking    32 non-null     float64 
#  
#  |    | categoria       | indicador   |   valor |   ranking |
#  |---:|:----------------|:------------|--------:|----------:|
#  |  0 | Serv. doméstico | Mujeres     | 97.2356 |         1 |
#  
#  ------------------------------
#  