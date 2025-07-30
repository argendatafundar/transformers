from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
	drop_col(col=['geocodigoFundar', 'es_agregacion', 'anio'], axis=1),
	rename_cols(map={'continente_fundar': 'grupo', 'geonombreFundar': 'geonombre'})
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
#  drop_col(col=['geocodigoFundar', 'es_agregacion', 'anio'], axis=1)
#  RangeIndex: 5095 entries, 0 to 5094
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    5095 non-null   object 
#   1   continente_fundar  4734 non-null   object 
#   2   idh                5095 non-null   float64
#   3   dif_idh_idhp       5095 non-null   float64
#  
#  |    | geonombreFundar   | continente_fundar   |   idh |   dif_idh_idhp |
#  |---:|:------------------|:--------------------|------:|---------------:|
#  |  0 | Afganistán        | Asia                | 0.284 |        1.05634 |
#  
#  ------------------------------
#  
#  rename_cols(map={'continente_fundar': 'grupo', 'geonombreFundar': 'geonombre'})
#  RangeIndex: 5095 entries, 0 to 5094
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geonombre     5095 non-null   object 
#   1   grupo         4734 non-null   object 
#   2   idh           5095 non-null   float64
#   3   dif_idh_idhp  5095 non-null   float64
#  
#  |    | geonombre   | grupo   |   idh |   dif_idh_idhp |
#  |---:|:------------|:--------|------:|---------------:|
#  |  0 | Afganistán  | Asia    | 0.284 |        1.05634 |
#  
#  ------------------------------
#  