from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
	query(condition='geocodigoFundar not in ["LAC", "TSA", "TSS", "SSA", "IBD","IDX", "MNA", "TLA", "SAS", "TEA", "TMN", "EAP", "TEA"]'),
	drop_col(col='iso3', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3255 entries, 0 to 3254
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             3255 non-null   object 
#   1   anio             3255 non-null   int64  
#   2   cambio_relativo  3255 non-null   float64
#   3   geocodigoFundar  3255 non-null   object 
#   4   geonombreFundar  3255 non-null   object 
#  
#  |    | iso3   |   anio |   cambio_relativo | geocodigoFundar   | geonombreFundar              |
#  |---:|:-------|-------:|------------------:|:------------------|:-----------------------------|
#  |  0 | AFE    |   2023 |         -0.021853 | AFE               | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar not in ["LAC", "TSA", "TSS", "SSA", "IBD","IDX", "MNA", "TLA", "SAS", "TEA", "TMN", "EAP", "TEA"]')
#  Index: 3099 entries, 0 to 3254
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             3099 non-null   object 
#   1   anio             3099 non-null   int64  
#   2   cambio_relativo  3099 non-null   float64
#   3   geocodigoFundar  3099 non-null   object 
#   4   geonombreFundar  3099 non-null   object 
#  
#  |    | iso3   |   anio |   cambio_relativo | geocodigoFundar   | geonombreFundar              |
#  |---:|:-------|-------:|------------------:|:------------------|:-----------------------------|
#  |  0 | AFE    |   2023 |         -0.021853 | AFE               | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 3099 entries, 0 to 3254
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             3099 non-null   int64  
#   1   cambio_relativo  3099 non-null   float64
#   2   geocodigoFundar  3099 non-null   object 
#   3   geonombreFundar  3099 non-null   object 
#  
#  |    |   anio |   cambio_relativo | geocodigoFundar   | geonombreFundar              |
#  |---:|-------:|------------------:|:------------------|:-----------------------------|
#  |  0 |   2023 |         -0.021853 | AFE               | África Oriental y Meridional |
#  
#  ------------------------------
#  