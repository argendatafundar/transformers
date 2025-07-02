from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    return df.rename(map)

@transformer.convert
def drop_col(df: pl.DataFrame, col, axis=1):
    # In Polars, axis parameter is not needed
    if isinstance(col, str):
        return df.drop(col)
    elif isinstance(col, list):
        return df.drop(col)
    else:
        return df.drop([col])

@transformer.convert
def sort_values_by_comparison(df: pl.DataFrame, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname + '_map'

    # Create mapping column using replace with default value for unmapped items
    df_ = df.with_columns(
        pl.col(colname).replace(precedence, default=float('inf')).alias(mapcol)
    )

    # Sort by prefix columns, mapped column, and suffix columns
    sort_columns = [*prefix, mapcol, *suffix]
    df_ = df_.sort(sort_columns)

    # Drop the temporary mapping column
    return df_.drop(mapcol)

@transformer.convert
def replace_value(df: pl.DataFrame, col: str, mapping: dict, alias: str = None):

    if not alias:
        alias = col

    df = df.with_columns(
        pl.col(col).replace(mapping).alias(alias)
    )

    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'valor_en_mtco2e': 'valor', 'subsector': 'indicador'}),
	drop_col(col='sector', axis=1),
	sort_values_by_comparison(colname='indicador', precedence={'Industrias de la energía': 0, 'Transporte': 1, 'Industrias manufactureras y de la construcción': 2, 'Otros sectores': 3, 'Emisiones fugitivas provenientes de la fabricación de combustibles': 4}, prefix=['anio'], suffix=[]),
	replace_value(col='indicador', mapping={'Industrias de la energía': 'Energía', 'Industrias manufactureras y de la construcción': 'Industria y construcción', 'Emisiones fugitivas provenientes de la fabricación de combustibles': 'Emisiones fugitivas'}, alias=None)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_mtco2e': 'valor', 'subsector': 'indicador'})
#  
#  ------------------------------
#  
#  drop_col(col='sector', axis=1)
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Industrias de la energía': 0, 'Transporte': 1, 'Industrias manufactureras y de la construcción': 2, 'Otros sectores': 3, 'Emisiones fugitivas provenientes de la fabricación de combustibles': 4}, prefix=['anio'], suffix=[])
#  
#  ------------------------------
#  
#  replace_value(col='indicador', mapping={'Industrias de la energía': 'Energía', 'Industrias manufactureras y de la construcción': 'Industria y construcción', 'Emisiones fugitivas provenientes de la fabricación de combustibles': 'Emisiones fugitivas'}, alias=None)
#  
#  ------------------------------
#  