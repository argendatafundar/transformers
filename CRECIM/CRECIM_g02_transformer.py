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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='nivel_agregacion', axis=1),
	drop_col(col='pais_nombre', axis=1),
	rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo'}),
	wide_to_long(primary_keys=['geocodigo', 'grupo', 'anio'], value_name='valor', var_name='indicador'),
	query(condition='anio == anio.max()'),
	drop_col(col='anio', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 5911 entries, 0 to 5910
#  Data columns (total 7 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   iso3                  5911 non-null   object 
#   1   pais_nombre           5911 non-null   object 
#   2   continente_fundar     5911 non-null   object 
#   3   anio                  5911 non-null   int64  
#   4   expectativa_al_nacer  5911 non-null   float64
#   5   pib_pc                5911 non-null   float64
#   6   nivel_agregacion      5911 non-null   object 
#  
#  |    | iso3   | pais_nombre   | continente_fundar                      |   anio |   expectativa_al_nacer |   pib_pc | nivel_agregacion   |
#  |---:|:-------|:--------------|:---------------------------------------|-------:|-----------------------:|---------:|:-------------------|
#  |  0 | ABW    | Aruba         | América del Norte, Central y el Caribe |   1990 |                  73.08 |  30823.5 | pais               |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_agregacion', axis=1)
#  RangeIndex: 5911 entries, 0 to 5910
#  Data columns (total 6 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   iso3                  5911 non-null   object 
#   1   pais_nombre           5911 non-null   object 
#   2   continente_fundar     5911 non-null   object 
#   3   anio                  5911 non-null   int64  
#   4   expectativa_al_nacer  5911 non-null   float64
#   5   pib_pc                5911 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar                      |   anio |   expectativa_al_nacer |   pib_pc |
#  |---:|:-------|:--------------|:---------------------------------------|-------:|-----------------------:|---------:|
#  |  0 | ABW    | Aruba         | América del Norte, Central y el Caribe |   1990 |                  73.08 |  30823.5 |
#  
#  ------------------------------
#  
#  drop_col(col='pais_nombre', axis=1)
#  RangeIndex: 5911 entries, 0 to 5910
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   iso3                  5911 non-null   object 
#   1   continente_fundar     5911 non-null   object 
#   2   anio                  5911 non-null   int64  
#   3   expectativa_al_nacer  5911 non-null   float64
#   4   pib_pc                5911 non-null   float64
#  
#  |    | iso3   | continente_fundar                      |   anio |   expectativa_al_nacer |   pib_pc |
#  |---:|:-------|:---------------------------------------|-------:|-----------------------:|---------:|
#  |  0 | ABW    | América del Norte, Central y el Caribe |   1990 |                  73.08 |  30823.5 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'continente_fundar': 'grupo'})
#  RangeIndex: 5911 entries, 0 to 5910
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geocodigo             5911 non-null   object 
#   1   grupo                 5911 non-null   object 
#   2   anio                  5911 non-null   int64  
#   3   expectativa_al_nacer  5911 non-null   float64
#   4   pib_pc                5911 non-null   float64
#  
#  |    | geocodigo   | grupo                                  |   anio |   expectativa_al_nacer |   pib_pc |
#  |---:|:------------|:---------------------------------------|-------:|-----------------------:|---------:|
#  |  0 | ABW         | América del Norte, Central y el Caribe |   1990 |                  73.08 |  30823.5 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geocodigo', 'grupo', 'anio'], value_name='valor', var_name='indicador')
#  RangeIndex: 11822 entries, 0 to 11821
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  11822 non-null  object 
#   1   grupo      11822 non-null  object 
#   2   anio       11822 non-null  int64  
#   3   indicador  11822 non-null  object 
#   4   valor      11822 non-null  float64
#  
#  |    | geocodigo   | grupo                                  |   anio | indicador            |   valor |
#  |---:|:------------|:---------------------------------------|-------:|:---------------------|--------:|
#  |  0 | ABW         | América del Norte, Central y el Caribe |   1990 | expectativa_al_nacer |   73.08 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 386 entries, 31 to 11821
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  386 non-null    object 
#   1   grupo      386 non-null    object 
#   2   anio       386 non-null    int64  
#   3   indicador  386 non-null    object 
#   4   valor      386 non-null    float64
#  
#  |    | geocodigo   | grupo                                  |   anio | indicador            |   valor |
#  |---:|:------------|:---------------------------------------|-------:|:---------------------|--------:|
#  | 31 | ABW         | América del Norte, Central y el Caribe |   2021 | expectativa_al_nacer |   74.63 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 386 entries, 31 to 11821
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  386 non-null    object 
#   1   grupo      386 non-null    object 
#   2   indicador  386 non-null    object 
#   3   valor      386 non-null    float64
#  
#  |    | geocodigo   | grupo                                  | indicador            |   valor |
#  |---:|:------------|:---------------------------------------|:---------------------|--------:|
#  | 31 | ABW         | América del Norte, Central y el Caribe | expectativa_al_nacer |   74.63 |
#  
#  ------------------------------
#  