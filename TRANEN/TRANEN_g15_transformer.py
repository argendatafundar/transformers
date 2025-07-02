from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_na(df: pl.DataFrame, cols: list):
    return df.drop_nulls(subset=cols)

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    return df.rename(map)

@transformer.convert
def drop_col(df: pl.DataFrame, col, axis=1):
    return df.drop(col)

@transformer.convert
def query(df: pl.DataFrame, condition: str):
    return df.filter(eval(condition))

@transformer.convert
def recalculo_kaya(df: pl.DataFrame, group: str, date_col: str):
    # Filter data from the year 1965 onwards
    data = df.filter(pl.col(date_col) >= 1965)

    # Get all column names except date_col and group
    variables = [col for col in data.columns if col not in [date_col, group]]

    # Sort by group and date to ensure proper ordering
    data = data.sort([group, date_col])

    # Calculate baseline values and transformations for each variable
    baseline_expressions = []
    transform_expressions = []

    for var in variables:
        # Try to process each variable, handling different data types
        try:
            # Attempt to cast to numeric if it's not already numeric
            if not data[var].dtype.is_numeric():
                # Try to cast string columns to numeric, skip if it fails
                numeric_col = pl.col(var).cast(pl.Float64, strict=False)
            else:
                numeric_col = pl.col(var)

            # Get the first non-null value for each group as baseline
            baseline_expr = (
                numeric_col
                .drop_nulls()
                .first()
                .over(group)
                .alias(f"{var}_baseline")
            )
            baseline_expressions.append(baseline_expr)

            # Calculate the percentage change: ((value / baseline - 1) * 100)
            # Handle division by zero and null baselines
            transform_expr = (
                pl.when(
                    pl.col(f"{var}_baseline").is_not_null() & 
                    (pl.col(f"{var}_baseline") != 0) &
                    numeric_col.is_not_null()
                )
                .then(((numeric_col / pl.col(f"{var}_baseline")) - 1) * 100)
                .otherwise(None)
            ).alias(f"{var}_index")
            transform_expressions.append(transform_expr)

        except Exception:
            # Skip variables that can't be processed (e.g., pure text columns)
            continue

    # Apply transformations if we have any
    if baseline_expressions and transform_expressions:
        result = (
            data
            .with_columns(baseline_expressions)  # Add baseline columns
            .with_columns(transform_expressions)  # Add transformed columns with "_index" suffix
            .drop([f"{var}_baseline" for var in variables if f"{var}_baseline" in [expr.meta.output_name() for expr in baseline_expressions]])  # Remove temporary baseline columns
        )
    else:
        # If no variables could be processed, return original data
        result = data

    return result

@transformer.convert
def replace_value(df: pl.DataFrame, col: str, curr_value: str, new_value: str):
    df = df.with_columns(pl.col(col).replace(curr_value, new_value))
    return df

@transformer.convert
def wide_to_long(df: pl.DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, variable_name=var_name)

@transformer.convert
def sort_values(df: pl.DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    return df.sort(by=by, descending=how == 'descending')
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	sort_values(how='ascending', by=['anio', 'geocodigoFundar']),
	drop_col(col='geonombreFundar', axis=1),
	recalculo_kaya(group='geocodigoFundar', date_col='anio'),
	query(condition='pl.col("geocodigoFundar") == "WLD"'),
	rename_cols(map={'geocodigoFundar': 'geocodigo'}),
	wide_to_long(primary_keys=['anio', 'geocodigo'], value_name='valor', var_name='categoria'),
	query(condition='pl.col("categoria").str.ends_with("_index")'),
	replace_value(col='categoria', curr_value='emision_anual_co2_ton_index', new_value='Emisiones anuales de CO2'),
	replace_value(col='categoria', curr_value='energia_por_unidad_pib_kwh_index', new_value='Intensidad energética'),
	replace_value(col='categoria', curr_value='pib_per_cap_usd_ppa_2011_index', new_value='PIB per cápita en dólares'),
	replace_value(col='categoria', curr_value='poblacion_idex', new_value='Población'),
	replace_value(col='categoria', curr_value='emision_anual_kgco2_por_kwh_index', new_value='Intensidad de carbono*'),
	replace_value(col='categoria', curr_value='emision_anual_kgco2_por_usd_ppa_2011_index', new_value='Intensidad de carbono**'),
	drop_col(col='geocodigo', axis=1),
	drop_na(cols='valor'),
	sort_values(how='ascending', by=['anio', 'categoria'])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigoFundar'])
#  
#  ------------------------------
#  
#  drop_col(col='geonombreFundar', axis=1)
#  
#  ------------------------------
#  
#  recalculo_kaya(group='geocodigoFundar', date_col='anio')
#  
#  ------------------------------
#  
#  query(condition='pl.col("geocodigoFundar") == "WLD"')
#  
#  ------------------------------
#  
#  rename_cols(map={'geocodigoFundar': 'geocodigo'})
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'geocodigo'], value_name='valor', var_name='categoria')
#  
#  ------------------------------
#  
#  query(condition='pl.col("categoria").str.ends_with("_index")')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='emision_anual_co2_ton_index', new_value='Emisiones anuales de CO2')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='energia_por_unidad_pib_kwh_index', new_value='Intensidad energética')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='pib_per_cap_usd_ppa_2011_index', new_value='PIB per cápita en dólares')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='poblacion_idex', new_value='Población')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='emision_anual_kgco2_por_kwh_index', new_value='Intensidad de carbono*')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='emision_anual_kgco2_por_usd_ppa_2011_index', new_value='Intensidad de carbono**')
#  
#  ------------------------------
#  
#  drop_col(col='geocodigo', axis=1)
#  
#  ------------------------------
#  
#  drop_na(cols='valor')
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'categoria'])
#  
#  ------------------------------
#  