from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition="anio == anio.max() & ~ iso3.isin(['ZZA.VHHD', 'ZZB.HHD', 'ZZC.MHD', 'ZZD.LHD', 'ZZE.AS', 'ZZF.EAP', 'ZZG.ECA', 'ZZH.LAC', 'ZZI.SA', 'ZZJ.SSA', 'ZZK.WORLD'])"),
	rename_cols(map={'iso3': 'geocodigo', 'idh': 'valor'}),
	drop_col(col=['pais_nombre', 'continente_fundar', 'es_agregacion'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6171 entries, 0 to 6170
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6171 non-null   object 
#   1   pais_nombre        6171 non-null   object 
#   2   continente_fundar  5808 non-null   object 
#   3   es_agregacion      5808 non-null   float64
#   4   anio               6171 non-null   int64  
#   5   idh                6171 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 | 0.284 |
#  
#  ------------------------------
#  
#  query(condition="anio == anio.max() & ~ iso3.isin(['ZZA.VHHD', 'ZZB.HHD', 'ZZC.MHD', 'ZZD.LHD', 'ZZE.AS', 'ZZF.EAP', 'ZZG.ECA', 'ZZH.LAC', 'ZZI.SA', 'ZZJ.SSA', 'ZZK.WORLD'])")
#  Index: 193 entries, 32 to 5807
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               193 non-null    object 
#   1   pais_nombre        193 non-null    object 
#   2   continente_fundar  193 non-null    object 
#   3   es_agregacion      193 non-null    float64
#   4   anio               193 non-null    int64  
#   5   idh                193 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 |   2022 | 0.462 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'idh': 'valor'})
#  Index: 193 entries, 32 to 5807
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigo          193 non-null    object 
#   1   pais_nombre        193 non-null    object 
#   2   continente_fundar  193 non-null    object 
#   3   es_agregacion      193 non-null    float64
#   4   anio               193 non-null    int64  
#   5   valor              193 non-null    float64
#  
#  |    | geocodigo   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   valor |
#  |---:|:------------|:--------------|:--------------------|----------------:|-------:|--------:|
#  | 32 | AFG         | Afganistán    | Asia                |               0 |   2022 |   0.462 |
#  
#  ------------------------------
#  
#  drop_col(col=['pais_nombre', 'continente_fundar', 'es_agregacion'], axis=1)
#  Index: 193 entries, 32 to 5807
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  193 non-null    object 
#   1   anio       193 non-null    int64  
#   2   valor      193 non-null    float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  | 32 | AFG         |   2022 |   0.462 |
#  
#  ------------------------------
#  