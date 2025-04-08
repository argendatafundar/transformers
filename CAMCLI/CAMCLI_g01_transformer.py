from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='iso3', axis=1),
	rename_cols(map={'continente_fundar': 'nivel1', 'iso3_desc_fundar': 'nivel2', 'valor_en_porcent': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 215 entries, 0 to 214
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               215 non-null    object 
#   1   continente_fundar  215 non-null    object 
#   2   iso3_desc_fundar   215 non-null    object 
#   3   anio               215 non-null    int64  
#   4   valor_en_porcent   215 non-null    float64
#  
#  |    | iso3   | continente_fundar   | iso3_desc_fundar   |   anio |   valor_en_porcent |
#  |---:|:-------|:--------------------|:-------------------|-------:|-------------------:|
#  |  0 | AFG    | Asia                | Afganistán         |   2023 |        0.000291605 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  RangeIndex: 215 entries, 0 to 214
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   continente_fundar  215 non-null    object 
#   1   iso3_desc_fundar   215 non-null    object 
#   2   anio               215 non-null    int64  
#   3   valor_en_porcent   215 non-null    float64
#  
#  |    | continente_fundar   | iso3_desc_fundar   |   anio |   valor_en_porcent |
#  |---:|:--------------------|:-------------------|-------:|-------------------:|
#  |  0 | Asia                | Afganistán         |   2023 |        0.000291605 |
#  
#  ------------------------------
#  
#  rename_cols(map={'continente_fundar': 'nivel1', 'iso3_desc_fundar': 'nivel2', 'valor_en_porcent': 'valor'})
#  RangeIndex: 215 entries, 0 to 214
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  215 non-null    object 
#   1   nivel2  215 non-null    object 
#   2   anio    215 non-null    int64  
#   3   valor   215 non-null    float64
#  
#  |    | nivel1   | nivel2     |   anio |     valor |
#  |---:|:---------|:-----------|-------:|----------:|
#  |  0 | Asia     | Afganistán |   2023 | 0.0291605 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 215 entries, 0 to 214
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  215 non-null    object 
#   1   nivel2  215 non-null    object 
#   2   anio    215 non-null    int64  
#   3   valor   215 non-null    float64
#  
#  |    | nivel1   | nivel2     |   anio |     valor |
#  |---:|:---------|:-----------|-------:|----------:|
#  |  0 | Asia     | Afganistán |   2023 | 0.0291605 |
#  
#  ------------------------------
#  