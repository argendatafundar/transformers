from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values_by_comparison(df: pl.DataFrame, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.clone()
    df_ = df_.with_columns(pl.col(colname).replace(precedence).alias(mapcol))
    df_ = df_.sort([*prefix, mapcol, *suffix])
    return df_.drop(mapcol)

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df

@transformer.convert
def drop_col(df: pl.DataFrame, col, axis=1):
    if isinstance(col, str):
        col = [col]
    return df.drop(col)

@transformer.convert
def df_sql(df: pl.DataFrame, query: str) -> pl.DataFrame: 
    df = df.sql(query)
    return df

@transformer.convert
def drop_na(df: pl.DataFrame, cols: list):
    return df.drop_nulls(subset=cols)

@transformer.convert
def replace_value(df: pl.DataFrame, col: str, curr_value: str, new_value: str):
    df = df.with_columns(pl.col(col).replace(curr_value, new_value))
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='geocodigoFundar', curr_value='OWID_KOS', new_value='XKX'),
	replace_value(col='geocodigoFundar', curr_value='OWID_WRL', new_value='WLD'),
	df_sql(query="select * from self where geocodigoFundar == 'ARG' and tipo_energia != 'Total'"),
	df_sql(query="select * from self where tipo_energia != 'Otras renovables'"),
	replace_value(col='tipo_energia', curr_value='Bioenergia', new_value='Bioenergía'),
	replace_value(col='tipo_energia', curr_value='Carbon', new_value='Carbón'),
	replace_value(col='tipo_energia', curr_value='Petroleo', new_value='Petróleo'),
	replace_value(col='tipo_energia', curr_value='Eolica', new_value='Eólica'),
	rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'}),
	drop_col(col=['geocodigoFundar', 'porcentaje'], axis=1),
	drop_na(cols=['valor']),
	sort_values_by_comparison(colname='indicador', precedence={'Bioenergía': 10, 'Otras renovables': 1, 'Biocombustibles': 2, 'Solar': 3, 'Eólica': 4, 'Nuclear': 5, 'Hidro': 6, 'Gas natural': 7, 'Petróleo': 8, 'Carbón': 9}, prefix=['anio'], suffix=[])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  replace_value(col='geocodigoFundar', curr_value='OWID_KOS', new_value='XKX')
#  
#  ------------------------------
#  
#  replace_value(col='geocodigoFundar', curr_value='OWID_WRL', new_value='WLD')
#  
#  ------------------------------
#  
#  df_sql(query="select * from self where geocodigoFundar == 'ARG' and tipo_energia != 'Total'")
#  
#  ------------------------------
#  
#  df_sql(query="select * from self where tipo_energia != 'Otras renovables'")
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Bioenergia', new_value='Bioenergía')
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Carbon', new_value='Carbón')
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Petroleo', new_value='Petróleo')
#  
#  ------------------------------
#  
#  replace_value(col='tipo_energia', curr_value='Eolica', new_value='Eólica')
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'})
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'porcentaje'], axis=1)
#  
#  ------------------------------
#  
#  drop_na(cols=['valor'])
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Bioenergía': 10, 'Otras renovables': 1, 'Biocombustibles': 2, 'Solar': 3, 'Eólica': 4, 'Nuclear': 5, 'Hidro': 6, 'Gas natural': 7, 'Petróleo': 8, 'Carbón': 9}, prefix=['anio'], suffix=[])
#  
#  ------------------------------
#  