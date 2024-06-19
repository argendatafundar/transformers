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
rename_cols(map={' ano': 'anio', 'variable': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 160 entries, 0 to 159
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype 
#  ---  ------    --------------  ----- 
#   0    ano      160 non-null    int64 
#   1   variable  160 non-null    object
#   2   valor     160 non-null    int64 
#  
#  |    |    ano | variable   |   valor |
#  |---:|-------:|:-----------|--------:|
#  |  0 |   1992 | Mujeres    |   11178 |
#  
#  ------------------------------
#  
#  rename_cols(map={' ano': 'anio', 'variable': 'indicador'})
#  RangeIndex: 160 entries, 0 to 159
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       160 non-null    int64 
#   1   indicador  160 non-null    object
#   2   valor      160 non-null    int64 
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Mujeres     |   11178 |
#  
#  ------------------------------
#  