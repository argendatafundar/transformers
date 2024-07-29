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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
	rename_cols(map={'iso3': 'geocodigo', 'pib_pc': 'valor'}),
	drop_col(col=['pais_nombre', 'continente_fundar', 'nivel_agregacion'], axis=1),
	query(condition="geocodigo not in ['LAC','TLA','SSA','TSS', 'EAP','ECA','MNA','TSA','TSS', 'TEC','TEA']")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7662 entries, 0 to 7661
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               7662 non-null   object 
#   1   pais_nombre        7662 non-null   object 
#   2   continente_fundar  6095 non-null   object 
#   3   anio               7662 non-null   int64  
#   4   pib_pc             7662 non-null   float64
#   5   nivel_agregacion   7662 non-null   object 
#  
#  |    | iso3   | pais_nombre   | continente_fundar                      |   anio |   pib_pc | nivel_agregacion   |
#  |---:|:-------|:--------------|:---------------------------------------|-------:|---------:|:-------------------|
#  |  0 | ABW    | Aruba         | América del Norte, Central y el Caribe |   1990 |  30823.5 | pais               |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 232 entries, 64 to 7661
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               232 non-null    object 
#   1   pais_nombre        232 non-null    object 
#   2   continente_fundar  184 non-null    object 
#   3   pib_pc             232 non-null    float64
#   4   nivel_agregacion   232 non-null    object 
#  
#  |    | iso3   | pais_nombre                  |   continente_fundar |   pib_pc | nivel_agregacion   |
#  |---:|:-------|:-----------------------------|--------------------:|---------:|:-------------------|
#  | 64 | AFE    | África Oriental y Meridional |                 nan |  3553.91 | agregacion         |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'pib_pc': 'valor'})
#  Index: 232 entries, 64 to 7661
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigo          232 non-null    object 
#   1   pais_nombre        232 non-null    object 
#   2   continente_fundar  184 non-null    object 
#   3   valor              232 non-null    float64
#   4   nivel_agregacion   232 non-null    object 
#  
#  |    | geocodigo   | pais_nombre                  |   continente_fundar |   valor | nivel_agregacion   |
#  |---:|:------------|:-----------------------------|--------------------:|--------:|:-------------------|
#  | 64 | AFE         | África Oriental y Meridional |                 nan | 3553.91 | agregacion         |
#  
#  ------------------------------
#  
#  drop_col(col=['pais_nombre', 'continente_fundar', 'nivel_agregacion'], axis=1)
#  Index: 232 entries, 64 to 7661
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  232 non-null    object 
#   1   valor      232 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 64 | AFE         | 3553.91 |
#  
#  ------------------------------
#  
#  query(condition="geocodigo not in ['LAC','TLA','SSA','TSS', 'EAP','ECA','MNA','TSA','TSS', 'TEC','TEA']")
#  Index: 222 entries, 64 to 7661
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  222 non-null    object 
#   1   valor      222 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 64 | AFE         | 3553.91 |
#  
#  ------------------------------
#  