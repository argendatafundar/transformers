from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='lall_desc_full', replacements={'Total manufacturas': 'Total', 'Manufacturas basadas en recursos naturales': 'Basadas en RRNN', 'Manufacturas de alta tecnología': 'Alta tecnología', 'Manufacturas de media tecnología': 'Media tecnología', 'Manufacturas de baja tecnología': 'Baja tecnología'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 310 entries, 0 to 309
#  Data columns (total 5 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            310 non-null    int64  
#   1   region          310 non-null    object 
#   2   lall_desc_full  310 non-null    object 
#   3   exportaciones   310 non-null    float64
#   4   prop_global     310 non-null    float64
#  
#  |    |   anio | region   | lall_desc_full     |   exportaciones |   prop_global |
#  |---:|-------:|:---------|:-------------------|----------------:|--------------:|
#  |  0 |   1962 | Asia     | Total manufacturas |     1.05438e+10 |       11.6651 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='lall_desc_full', replacements={'Total manufacturas': 'Total', 'Manufacturas basadas en recursos naturales': 'Basadas en RRNN', 'Manufacturas de alta tecnología': 'Alta tecnología', 'Manufacturas de media tecnología': 'Media tecnología', 'Manufacturas de baja tecnología': 'Baja tecnología'})
#  RangeIndex: 310 entries, 0 to 309
#  Data columns (total 5 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   anio            310 non-null    int64  
#   1   region          310 non-null    object 
#   2   lall_desc_full  310 non-null    object 
#   3   exportaciones   310 non-null    float64
#   4   prop_global     310 non-null    float64
#  
#  |    |   anio | region   | lall_desc_full   |   exportaciones |   prop_global |
#  |---:|-------:|:---------|:-----------------|----------------:|--------------:|
#  |  0 |   1962 | Asia     | Total            |     1.05438e+10 |       11.6651 |
#  
#  ------------------------------
#  