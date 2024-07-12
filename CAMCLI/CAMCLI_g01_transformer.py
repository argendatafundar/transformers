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
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='iso3', axis=1),
	rename_cols(map={'continente_fundar': 'nivel1', 'iso3_desc_fundar': 'nivel2', 'valor_en_porcent': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 214 entries, 0 to 213
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               214 non-null    object 
#   1   continente_fundar  214 non-null    object 
#   2   iso3_desc_fundar   214 non-null    object 
#   3   anio               214 non-null    int64  
#   4   valor_en_porcent   214 non-null    float64
#  
#  |    | iso3   | continente_fundar   | iso3_desc_fundar   |   anio |   valor_en_porcent |
#  |---:|:-------|:--------------------|:-------------------|-------:|-------------------:|
#  |  0 | AFG    | Asia                | Afganistán         |   2022 |        0.000326999 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  RangeIndex: 214 entries, 0 to 213
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   continente_fundar  214 non-null    object 
#   1   iso3_desc_fundar   214 non-null    object 
#   2   anio               214 non-null    int64  
#   3   valor_en_porcent   214 non-null    float64
#  
#  |    | continente_fundar   | iso3_desc_fundar   |   anio |   valor_en_porcent |
#  |---:|:--------------------|:-------------------|-------:|-------------------:|
#  |  0 | Asia                | Afganistán         |   2022 |        0.000326999 |
#  
#  ------------------------------
#  
#  rename_cols(map={'continente_fundar': 'nivel1', 'iso3_desc_fundar': 'nivel2', 'valor_en_porcent': 'valor'})
#  RangeIndex: 214 entries, 0 to 213
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  214 non-null    object 
#   1   nivel2  214 non-null    object 
#   2   anio    214 non-null    int64  
#   3   valor   214 non-null    float64
#  
#  |    | nivel1   | nivel2     |   anio |     valor |
#  |---:|:---------|:-----------|-------:|----------:|
#  |  0 | Asia     | Afganistán |   2022 | 0.0326999 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 214 entries, 0 to 213
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  214 non-null    object 
#   1   nivel2  214 non-null    object 
#   2   anio    214 non-null    int64  
#   3   valor   214 non-null    float64
#  
#  |    | nivel1   | nivel2     |   anio |     valor |
#  |---:|:---------|:-----------|-------:|----------:|
#  |  0 | Asia     | Afganistán |   2022 | 0.0326999 |
#  
#  ------------------------------
#  