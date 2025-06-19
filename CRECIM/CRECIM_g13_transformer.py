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
	query(condition='anio == anio.max()'),
	query(condition='geocodigoFundar not in ["LAC", "TSA", "TSS", "SSA", "IBD","IDX", "MNA", "TLA", "SAS", "TEA", "TMN", "EAP", "TEA"]'),
	drop_col(col='iso3', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 8083 entries, 0 to 8082
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             8083 non-null   object 
#   1   anio             8083 non-null   int64  
#   2   pib_pc           8083 non-null   float64
#   3   geocodigoFundar  8083 non-null   object 
#   4   geonombreFundar  8083 non-null   object 
#  
#  |    | iso3   |   anio |   pib_pc | geocodigoFundar   | geonombreFundar              |
#  |---:|:-------|-------:|---------:|:------------------|:-----------------------------|
#  |  0 | AFE    |   2023 |  3967.86 | AFE               | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 236 entries, 0 to 8049
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             236 non-null    object 
#   1   anio             236 non-null    int64  
#   2   pib_pc           236 non-null    float64
#   3   geocodigoFundar  236 non-null    object 
#   4   geonombreFundar  236 non-null    object 
#  
#  |    | iso3   |   anio |   pib_pc | geocodigoFundar   | geonombreFundar              |
#  |---:|:-------|-------:|---------:|:------------------|:-----------------------------|
#  |  0 | AFE    |   2023 |  3967.86 | AFE               | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar not in ["LAC", "TSA", "TSS", "SSA", "IBD","IDX", "MNA", "TLA", "SAS", "TEA", "TMN", "EAP", "TEA"]')
#  Index: 224 entries, 0 to 8049
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             224 non-null    object 
#   1   anio             224 non-null    int64  
#   2   pib_pc           224 non-null    float64
#   3   geocodigoFundar  224 non-null    object 
#   4   geonombreFundar  224 non-null    object 
#  
#  |    | iso3   |   anio |   pib_pc | geocodigoFundar   | geonombreFundar              |
#  |---:|:-------|-------:|---------:|:------------------|:-----------------------------|
#  |  0 | AFE    |   2023 |  3967.86 | AFE               | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 224 entries, 0 to 8049
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             224 non-null    int64  
#   1   pib_pc           224 non-null    float64
#   2   geocodigoFundar  224 non-null    object 
#   3   geonombreFundar  224 non-null    object 
#  
#  |    |   anio |   pib_pc | geocodigoFundar   | geonombreFundar              |
#  |---:|-------:|---------:|:------------------|:-----------------------------|
#  |  0 |   2023 |  3967.86 | AFE               | África Oriental y Meridional |
#  
#  ------------------------------
#  