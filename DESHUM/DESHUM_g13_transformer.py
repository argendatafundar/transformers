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

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	drop_col(col=['geocodigoFundar', 'es_agregacion', 'anio'], axis=1),
	rename_cols(map={'continente_fundar': 'grupo', 'geonombreFundar': 'geonombre'}),
	drop_na(col=['grupo']),
	wide_to_long(primary_keys=['grupo', 'geonombre'], value_name='valor', var_name='indicador')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 5095 entries, 0 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    5095 non-null   object 
#   1   geonombreFundar    5095 non-null   object 
#   2   continente_fundar  4734 non-null   object 
#   3   es_agregacion      4734 non-null   float64
#   4   anio               5095 non-null   int64  
#   5   idh                5095 non-null   float64
#   6   dif_idh_idhp       5095 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|------:|---------------:|
#  |  0 | AFG               | Afganistán        | Asia                |               0 |   1990 | 0.284 |        1.05634 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 165 entries, 32 to 5094
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    165 non-null    object 
#   1   geonombreFundar    165 non-null    object 
#   2   continente_fundar  154 non-null    object 
#   3   es_agregacion      154 non-null    float64
#   4   anio               165 non-null    int64  
#   5   idh                165 non-null    float64
#   6   dif_idh_idhp       165 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   es_agregacion |   anio |   idh |   dif_idh_idhp |
#  |---:|:------------------|:------------------|:--------------------|----------------:|-------:|------:|---------------:|
#  | 32 | AFG               | Afganistán        | Asia                |               0 |   2022 | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar', 'es_agregacion', 'anio'], axis=1)
#  Index: 165 entries, 32 to 5094
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    165 non-null    object 
#   1   continente_fundar  154 non-null    object 
#   2   idh                165 non-null    float64
#   3   dif_idh_idhp       165 non-null    float64
#  
#  |    | geonombreFundar   | continente_fundar   |   idh |   dif_idh_idhp |
#  |---:|:------------------|:--------------------|------:|---------------:|
#  | 32 | Afganistán        | Asia                | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  rename_cols(map={'continente_fundar': 'grupo', 'geonombreFundar': 'geonombre'})
#  Index: 165 entries, 32 to 5094
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geonombre     165 non-null    object 
#   1   grupo         154 non-null    object 
#   2   idh           165 non-null    float64
#   3   dif_idh_idhp  165 non-null    float64
#  
#  |    | geonombre   | grupo   |   idh |   dif_idh_idhp |
#  |---:|:------------|:--------|------:|---------------:|
#  | 32 | Afganistán  | Asia    | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  drop_na(col=['grupo'])
#  Index: 154 entries, 32 to 4733
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geonombre     154 non-null    object 
#   1   grupo         154 non-null    object 
#   2   idh           154 non-null    float64
#   3   dif_idh_idhp  154 non-null    float64
#  
#  |    | geonombre   | grupo   |   idh |   dif_idh_idhp |
#  |---:|:------------|:--------|------:|---------------:|
#  | 32 | Afganistán  | Asia    | 0.462 |       0.649351 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['grupo', 'geonombre'], value_name='valor', var_name='indicador')
#  RangeIndex: 308 entries, 0 to 307
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      308 non-null    object 
#   1   geonombre  308 non-null    object 
#   2   indicador  308 non-null    object 
#   3   valor      308 non-null    float64
#  
#  |    | grupo   | geonombre   | indicador   |   valor |
#  |---:|:--------|:------------|:------------|--------:|
#  |  0 | Asia    | Afganistán  | idh         |   0.462 |
#  
#  ------------------------------
#  