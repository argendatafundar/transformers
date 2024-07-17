from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='continente_fundar', axis=1),
	drop_col(col='nivel_agregacion', axis=1),
	drop_col(col='pais_nombre', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'pib_pc': 'valor'}),
	query(condition="~ geocodigo.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TEC', 'ECA', 'TSA','TEA', 'EAP'])")
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
#  drop_col(col='continente_fundar', axis=1)
#  RangeIndex: 7662 entries, 0 to 7661
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              7662 non-null   object 
#   1   pais_nombre       7662 non-null   object 
#   2   anio              7662 non-null   int64  
#   3   pib_pc            7662 non-null   float64
#   4   nivel_agregacion  7662 non-null   object 
#  
#  |    | iso3   | pais_nombre   |   anio |   pib_pc | nivel_agregacion   |
#  |---:|:-------|:--------------|-------:|---------:|:-------------------|
#  |  0 | ABW    | Aruba         |   1990 |  30823.5 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  RangeIndex: 7662 entries, 0 to 7661
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         7662 non-null   object 
#   1   pais_nombre  7662 non-null   object 
#   2   anio         7662 non-null   int64  
#   3   pib_pc       7662 non-null   float64
#  
#  |    | iso3   | pais_nombre   |   anio |   pib_pc |
#  |---:|:-------|:--------------|-------:|---------:|
#  |  0 | ABW    | Aruba         |   1990 |  30823.5 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  RangeIndex: 7662 entries, 0 to 7661
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   iso3    7662 non-null   object 
#   1   anio    7662 non-null   int64  
#   2   pib_pc  7662 non-null   float64
#  
#  |    | iso3   |   anio |   pib_pc |
#  |---:|:-------|-------:|---------:|
#  |  0 | ABW    |   1990 |  30823.5 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'pib_pc': 'valor'})
#  RangeIndex: 7662 entries, 0 to 7661
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7662 non-null   object 
#   1   anio       7662 non-null   int64  
#   2   valor      7662 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ABW         |   1990 | 30823.5 |
#  
#  ------------------------------
#  
#  query(condition="~ geocodigo.isin(['SSA', 'TMN','MNA', 'TSS', 'LAC', 'TLA', 'TEC', 'ECA', 'TSA','TEA', 'EAP'])")
#  Index: 7299 entries, 0 to 7661
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  7299 non-null   object 
#   1   anio       7299 non-null   int64  
#   2   valor      7299 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ABW         |   1990 | 30823.5 |
#  
#  ------------------------------
#  