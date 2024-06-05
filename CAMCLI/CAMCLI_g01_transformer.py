from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geoselector', 'continente_fundar': 'subgeoselector', 'iso3_desc_fundar': 'subsubgeoselector', 'valor_en_porcent': 'valor'})
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
#  rename_cols(map={'iso3': 'geoselector', 'continente_fundar': 'subgeoselector', 'iso3_desc_fundar': 'subsubgeoselector', 'valor_en_porcent': 'valor'})
#  RangeIndex: 219 entries, 0 to 218
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geoselector        219 non-null    object 
#   1   subgeoselector     219 non-null    object 
#   2   subsubgeoselector  219 non-null    object 
#   3   anio               219 non-null    int64  
#   4   valor              219 non-null    float64
#  
#  |    | geoselector   | subgeoselector   | subsubgeoselector   |   anio |       valor |
#  |---:|:--------------|:-----------------|:--------------------|-------:|------------:|
#  |  0 | AFG           | Asia             | Afganistán          |   2021 | 0.000319854 |
#  
#  ------------------------------
#  