from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='iso3 not in ["LAC", "TSA", "TSS", "SSA", "IBD","IDX", "MNA", "TLA", "SAS", "TEA", "TMN", "EAP", "TEA"]'),
	drop_col(col='iso3', axis=1),
	drop_col(col='pib_pc', axis=1),
	sort_values(how='ascending', by=['anio']),
	multiplicar_por_escalar(col='cambio_relativo', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 8962 entries, 0 to 8961
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             8962 non-null   object 
#   1   anio             8962 non-null   int64  
#   2   pib_pc           8962 non-null   float64
#   3   cambio_relativo  8962 non-null   float64
#   4   geocodigoFundar  8962 non-null   object 
#   5   geonombreFundar  8962 non-null   object 
#  
#  |    | iso3   |   anio |   pib_pc |   cambio_relativo | geocodigoFundar   | geonombreFundar              |
#  |---:|:-------|-------:|---------:|------------------:|:------------------|:-----------------------------|
#  |  0 | AFE    |   2023 |  1418.36 |       -0.00992672 | AFE               | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  query(condition='iso3 not in ["LAC", "TSA", "TSS", "SSA", "IBD","IDX", "MNA", "TLA", "SAS", "TEA", "TMN", "EAP", "TEA"]')
#  Index: 8374 entries, 0 to 8961
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   iso3             8374 non-null   object 
#   1   anio             8374 non-null   int64  
#   2   pib_pc           8374 non-null   float64
#   3   cambio_relativo  8374 non-null   float64
#   4   geocodigoFundar  8374 non-null   object 
#   5   geonombreFundar  8374 non-null   object 
#  
#  |    | iso3   |   anio |   pib_pc |   cambio_relativo | geocodigoFundar   | geonombreFundar              |
#  |---:|:-------|-------:|---------:|------------------:|:------------------|:-----------------------------|
#  |  0 | AFE    |   2023 |  1418.36 |       -0.00992672 | AFE               | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 8374 entries, 0 to 8961
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             8374 non-null   int64  
#   1   pib_pc           8374 non-null   float64
#   2   cambio_relativo  8374 non-null   float64
#   3   geocodigoFundar  8374 non-null   object 
#   4   geonombreFundar  8374 non-null   object 
#  
#  |    |   anio |   pib_pc |   cambio_relativo | geocodigoFundar   | geonombreFundar              |
#  |---:|-------:|---------:|------------------:|:------------------|:-----------------------------|
#  |  0 |   2023 |  1418.36 |       -0.00992672 | AFE               | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  drop_col(col='pib_pc', axis=1)
#  Index: 8374 entries, 0 to 8961
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             8374 non-null   int64  
#   1   cambio_relativo  8374 non-null   float64
#   2   geocodigoFundar  8374 non-null   object 
#   3   geonombreFundar  8374 non-null   object 
#  
#  |    |   anio |   cambio_relativo | geocodigoFundar   | geonombreFundar              |
#  |---:|-------:|------------------:|:------------------|:-----------------------------|
#  |  0 |   2023 |       -0.00992672 | AFE               | África Oriental y Meridional |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 8374 entries, 0 to 8373
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             8374 non-null   int64  
#   1   cambio_relativo  8374 non-null   float64
#   2   geocodigoFundar  8374 non-null   object 
#   3   geonombreFundar  8374 non-null   object 
#  
#  |    |   anio |   cambio_relativo | geocodigoFundar   | geonombreFundar   |
#  |---:|-------:|------------------:|:------------------|:------------------|
#  |  0 |   1975 |                 0 | ZWE               | Zimbabwe          |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='cambio_relativo', k=100)
#  RangeIndex: 8374 entries, 0 to 8373
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             8374 non-null   int64  
#   1   cambio_relativo  8374 non-null   float64
#   2   geocodigoFundar  8374 non-null   object 
#   3   geonombreFundar  8374 non-null   object 
#  
#  |    |   anio |   cambio_relativo | geocodigoFundar   | geonombreFundar   |
#  |---:|-------:|------------------:|:------------------|:------------------|
#  |  0 |   1975 |                 0 | ZWE               | Zimbabwe          |
#  
#  ------------------------------
#  