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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
drop_col(col='iso3', axis=1),
	rename_cols(map={'continente_fundar': 'categoria', 'iso3_desc_fundar': 'subcategoria', 'valor_en_porcent': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 219 entries, 0 to 218
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               219 non-null    object 
#   1   continente_fundar  219 non-null    object 
#   2   iso3_desc_fundar   219 non-null    object 
#   3   anio               219 non-null    int64  
#   4   valor_en_porcent   219 non-null    float64
#  
#  |    | iso3   | continente_fundar   | iso3_desc_fundar   |   anio |   valor_en_porcent |
#  |---:|:-------|:--------------------|:-------------------|-------:|-------------------:|
#  |  0 | AFG    | Asia                | Afganistán         |   2021 |        0.000319854 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  RangeIndex: 219 entries, 0 to 218
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   continente_fundar  219 non-null    object 
#   1   iso3_desc_fundar   219 non-null    object 
#   2   anio               219 non-null    int64  
#   3   valor_en_porcent   219 non-null    float64
#  
#  |    | continente_fundar   | iso3_desc_fundar   |   anio |   valor_en_porcent |
#  |---:|:--------------------|:-------------------|-------:|-------------------:|
#  |  0 | Asia                | Afganistán         |   2021 |        0.000319854 |
#  
#  ------------------------------
#  
#  rename_cols(map={'continente_fundar': 'categoria', 'iso3_desc_fundar': 'subcategoria', 'valor_en_porcent': 'valor'})
#  RangeIndex: 219 entries, 0 to 218
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   categoria     219 non-null    object 
#   1   subcategoria  219 non-null    object 
#   2   anio          219 non-null    int64  
#   3   valor         219 non-null    float64
#  
#  |    | categoria   | subcategoria   |   anio |       valor |
#  |---:|:------------|:---------------|-------:|------------:|
#  |  0 | Asia        | Afganistán     |   2021 | 0.000319854 |
#  
#  ------------------------------
#  