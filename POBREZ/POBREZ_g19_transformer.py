from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df:pl.DataFrame, cols:list):
    df = df.drop_nans(subset=cols)
    df = df.drop_nulls(subset=cols)
    return df

@transformer.convert
def df_sql(df: pl.DataFrame, query: str) -> pl.DataFrame: 
    df = df.sql(query)
    return df

@transformer.convert
def concatenar_columnas(df: pl.DataFrame, cols: list, nueva_col: str, separtor: str = "-") -> pl.DataFrame:
    # Validate that all columns exist
    missing_cols = [col for col in cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

    return df.with_columns([
        pl.concat_str(cols, separator=separtor).alias(nueva_col)
    ])

@transformer.convert
def expand_and_fill(df: pl.DataFrame, agrupacion: list, index: dict, objetivo: dict) -> pl.DataFrame:
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
    # Validate that all grouping columns exist
    missing_group_cols = [col for col in agrupacion if col not in df.columns]
    if missing_group_cols:
        raise ValueError(f"Grouping columns not found in DataFrame: {missing_group_cols}")

    # Validate that all index columns exist
    missing_index_cols = [col for col in index.keys() if col not in df.columns]
    if missing_index_cols:
        raise ValueError(f"Index columns not found in DataFrame: {missing_index_cols}")

    # Validate that all target columns exist
    missing_target_cols = [col for col in objetivo.keys() if col not in df.columns]
    if missing_target_cols:
        raise ValueError(f"Target columns not found in DataFrame: {missing_target_cols}")

    # Create all possible combinations of index values
    index_combinations = []
    index_cols = list(index.keys())
    index_values = list(index.values())

    # Generate cartesian product of all index values
    from itertools import product
    for combination in product(*index_values):
        index_combinations.append(dict(zip(index_cols, combination)))

    # Create a DataFrame with all index combinations
    index_df = pl.DataFrame(index_combinations)

    # Get unique groups from the original DataFrame
    groups_df = df.select(agrupacion).unique()

    # Create cartesian product using join with dummy column
    # Add a dummy column to both DataFrames for the join
    groups_df = groups_df.with_columns(pl.lit(1).alias("_dummy"))
    index_df = index_df.with_columns(pl.lit(1).alias("_dummy"))

    # Join to create cartesian product
    expanded_df = groups_df.join(index_df, on="_dummy", how="inner")

    # Remove the dummy column
    expanded_df = expanded_df.drop("_dummy")

    # Join with original DataFrame to preserve existing values
    result_df = expanded_df.join(
        df, 
        on=agrupacion + index_cols, 
        how='left'
    )

    # Fill missing values in target columns with default values
    # Only for non-None default values
    for target_col, default_value in objetivo.items():
        if default_value is not None:
            result_df = result_df.with_columns([
                pl.col(target_col).fill_null(default_value)
            ])

    return result_df

@transformer.convert
def replace_value(df: pl.DataFrame, col: str, mapping: dict, alias: str = None):

    if not alias:
        alias = col

    df = df.with_columns(
        pl.col(col).replace(mapping).alias(alias)
    )

    return df

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_na(cols=['year', 'poverty_rate']),
	expand_and_fill(agrupacion=['year', 'age_group', 'poverty_line'], index={'semester': ['I', 'II']}, objetivo={'poverty_rate': None}),
	replace_value(col='semester', mapping={'I': 1, 'II': 2}, alias=None),
	concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-'),
	df_sql(query="select * from self where poverty_line == 'Pobreza' and aniosem != '2024-2' "),
	replace_value(col='age_group', mapping={'0_14_years': '0-14', '15_29_years': '15-29', '30_64_years': '30-64', '65_and_more_years': '65 y más'}, alias=None),
	rename_cols(map={'age_group': 'categoria', 'poverty_rate': 'valor'}),
	df_sql(query='SELECT * FROM self ORDER BY categoria, year, semester')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  drop_na(cols=['year', 'poverty_rate'])
#  
#  ------------------------------
#  
#  expand_and_fill(agrupacion=['year', 'age_group', 'poverty_line'], index={'semester': ['I', 'II']}, objetivo={'poverty_rate': None})
#  
#  ------------------------------
#  
#  replace_value(col='semester', mapping={'I': 1, 'II': 2}, alias=None)
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semester'], nueva_col='aniosem', separtor='-')
#  
#  ------------------------------
#  
#  df_sql(query="select * from self where poverty_line == 'Pobreza' and aniosem != '2024-2' ")
#  
#  ------------------------------
#  
#  replace_value(col='age_group', mapping={'0_14_years': '0-14', '15_29_years': '15-29', '30_64_years': '30-64', '65_and_more_years': '65 y más'}, alias=None)
#  
#  ------------------------------
#  
#  rename_cols(map={'age_group': 'categoria', 'poverty_rate': 'valor'})
#  
#  ------------------------------
#  
#  df_sql(query='SELECT * FROM self ORDER BY categoria, year, semester')
#  
#  ------------------------------
#  