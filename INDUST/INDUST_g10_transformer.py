from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['vabpb_industrial', 'poblacion', 'vab_indust_pc'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 125 entries, 0 to 124
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   anio                  125 non-null    int64  
#   1   vabpb_industrial      125 non-null    float64
#   2   poblacion             125 non-null    int64  
#   3   vab_indust_pc         125 non-null    float64
#   4   vab_indust_pc_indice  125 non-null    float64
#  
#  |    |   anio |   vabpb_industrial |   poblacion |   vab_indust_pc |   vab_indust_pc_indice |
#  |---:|-------:|-------------------:|------------:|----------------:|-----------------------:|
#  |  0 |   1900 |            2455.42 | 4.69272e+06 |          523.24 |                18.1856 |
#  
#  ------------------------------
#  
#  drop_col(col=['vabpb_industrial', 'poblacion', 'vab_indust_pc'], axis=1)
#  RangeIndex: 125 entries, 0 to 124
#  Data columns (total 2 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   anio                  125 non-null    int64  
#   1   vab_indust_pc_indice  125 non-null    float64
#  
#  |    |   anio |   vab_indust_pc_indice |
#  |---:|-------:|-----------------------:|
#  |  0 |   1900 |                18.1856 |
#  
#  ------------------------------
#  