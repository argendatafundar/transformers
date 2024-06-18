from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df

@transformer.convert
def drop_cols(df, cols_to_drop):
    return df[list(set(df.columns) - set(cols_to_drop))]
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(iso3='geocodigo', cambio_relativo='valor'),
	drop_cols(cols_to_drop=['continente_fundar', 'pais_nombre', 'es_agregacion', 'pib_per_capita'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               7831 non-null   object 
#   1   pais_nombre        7831 non-null   object 
#   2   continente_fundar  7611 non-null   object 
#   3   es_agregacion      7831 non-null   int64  
#   4   anio               7831 non-null   int64  
#   5   pib_per_capita     7831 non-null   float64
#   6   cambio_relativo    7831 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   pib_per_capita |   cambio_relativo |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|-----------------:|------------------:|
#  |  0 | ARG    | Argentina     | América del Sur     |               0 |   1820 |             1591 |                 0 |
#  
#  ------------------------------
#  
#  rename_columns(iso3='geocodigo', cambio_relativo='valor')
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigo          7831 non-null   object 
#   1   pais_nombre        7831 non-null   object 
#   2   continente_fundar  7611 non-null   object 
#   3   es_agregacion      7831 non-null   int64  
#   4   anio               7831 non-null   int64  
#   5   pib_per_capita     7831 non-null   float64
#   6   valor              7831 non-null   float64
#  
#  |    | geocodigo   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   pib_per_capita |   valor |
#  |---:|:------------|:--------------|:--------------------|----------------:|-------:|-----------------:|--------:|
#  |  0 | ARG         | Argentina     | América del Sur     |               0 |   1820 |             1591 |       0 |
#  
#  ------------------------------
#  
#  drop_cols(cols_to_drop=['continente_fundar', 'pais_nombre', 'es_agregacion', 'pib_per_capita'])
#  RangeIndex: 7831 entries, 0 to 7830
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7831 non-null   object 
#   1   valor      7831 non-null   float64
#   2   anio       7831 non-null   int64  
#  
#  |    | geocodigo   |   valor |   anio |
#  |---:|:------------|--------:|-------:|
#  |  0 | ARG         |       0 |   1820 |
#  
#  ------------------------------
#  