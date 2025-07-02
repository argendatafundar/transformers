from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: pl.DataFrame, col, axis=1):
    return df.drop(col)

@transformer.convert
def replace_value(df: pl.DataFrame, col: str, curr_value: str, new_value: str):
    df = df.with_columns(pl.col(col).replace(curr_value, new_value))
    return df

@transformer.convert
def sort_values(df: pl.DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    return df.sort(by=by, descending=how == 'descending')

@transformer.convert
def drop_na(df: pl.DataFrame, cols: list):
    return df.drop_nulls(subset=cols)

@transformer.convert
def recalculo_kaya(df: pl.DataFrame, group: str, date_col: str):
    # Filter data from the year 1965 onwards
    data = df.filter(pl.col(date_col) >= 1965)

    # Get all column names except date_col and group
    variables = [col for col in data.columns if col not in [date_col, group]]

    # Get unique codes
    codes = data.get_column(group).unique()

    # Initialize empty list for results
    data_list = []

    # Process each code
    for j in codes:
        # Filter data for current code
        data_code = data.filter(pl.col(group) == j).sort(date_col)

        # Create a copy to modify
        data_code_modified = data_code.clone()

        # Process each variable
        for i in variables:
            # Drop nulls for current variable
            datos = data_code.drop_nulls(subset=[i])

            if datos.height == 0:
                # Skip this variable for this code if no data
                continue

            # Get baseline value
            baseline = datos.sort(date_col).slice(0,1).get_column(i)[0]

            try:
                # Calculate index only if we have the column
                if i in data_code_modified.columns:
                    data_code_modified = data_code_modified.with_columns(
                        ((pl.col(i) / baseline - 1) * 100).alias(i)
                    )
            except:
                # Skip if calculation fails
                continue

        data_list.append(data_code_modified)

    # Combine results
    return pl.concat(data_list)

@transformer.convert
def query(df: pl.DataFrame, condition: str):
    return df.filter(eval(condition))

@transformer.convert
def wide_to_long(df: pl.DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, variable_name=var_name)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col='geonombreFundar', axis=1),
	recalculo_kaya(group='geocodigoFundar', date_col='anio'),
	query(condition='pl.col("geocodigoFundar") == "WLD"'),
	wide_to_long(primary_keys=['anio', 'geocodigoFundar'], value_name='valor', var_name='categoria'),
	replace_value(col='categoria', curr_value='emision_anual_co2_ton', new_value='Emisiones anuales de CO2'),
	replace_value(col='categoria', curr_value='energia_por_unidad_pib_kwh', new_value='Intensidad energética'),
	replace_value(col='categoria', curr_value='pib_per_cap_usd_ppa_2011', new_value='PIB per cápita en dólares'),
	replace_value(col='categoria', curr_value='poblacion', new_value='Población'),
	replace_value(col='categoria', curr_value='emision_anual_kgco2_por_kwh', new_value='Intensidad de carbono*'),
	replace_value(col='categoria', curr_value='emision_anual_kgco2_por_usd_ppa_2011', new_value='Intensidad de carbono**'),
	drop_col(col='geocodigoFundar', axis=1),
	drop_na(cols='valor'),
	sort_values(how='ascending', by=['anio', 'categoria'])
)
#  PIPELINE_END


#  start()
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
#  wide_to_long(primary_keys=['anio', 'geocodigoFundar'], value_name='valor', var_name='categoria')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='emision_anual_co2_ton', new_value='Emisiones anuales de CO2')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='energia_por_unidad_pib_kwh', new_value='Intensidad energética')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='pib_per_cap_usd_ppa_2011', new_value='PIB per cápita en dólares')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='poblacion', new_value='Población')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='emision_anual_kgco2_por_kwh', new_value='Intensidad de carbono*')
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='emision_anual_kgco2_por_usd_ppa_2011', new_value='Intensidad de carbono**')
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
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