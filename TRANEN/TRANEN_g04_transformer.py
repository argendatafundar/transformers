from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: pl.DataFrame, by, descending = None):
    if not descending:
        descending = [False] * len(by)
    df = df.sort(by = by, descending= descending)
    return df

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(map)
    return df

@transformer.convert
def drop_na(df:pl.DataFrame, cols:list):
    df = df.drop_nans(subset=cols)
    df = df.drop_nulls(subset=cols)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'valor_en_porcentaje': 'valor', 'geocodigoFundar': 'geocodigo'}),
	drop_na(cols='valor'),
	sort_values(by=['geocodigo', 'anio'], descending=[False, False])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_porcentaje': 'valor', 'geocodigoFundar': 'geocodigo'})
#  
#  ------------------------------
#  
#  drop_na(cols='valor')
#  
#  ------------------------------
#  
#  sort_values(by=['geocodigo', 'anio'], descending=[False, False])
#  
#  ------------------------------
#  