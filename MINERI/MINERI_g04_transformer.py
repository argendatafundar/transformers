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
rename_cols(map={'grupo_nuevo': 'indicador', 'expo_grupo': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 139 entries, 0 to 138
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   grupo_nuevo  139 non-null    object 
#   1   anio         139 non-null    int64  
#   2   expo_grupo   139 non-null    float64
#  
#  |    | grupo_nuevo   |   anio |   expo_grupo |
#  |---:|:--------------|-------:|-------------:|
#  |  0 | cobre         |   1994 |  3.63523e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'grupo_nuevo': 'indicador', 'expo_grupo': 'valor'})
#  RangeIndex: 139 entries, 0 to 138
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  139 non-null    object 
#   1   anio       139 non-null    int64  
#   2   valor      139 non-null    float64
#  
#  |    | indicador   |   anio |       valor |
#  |---:|:------------|-------:|------------:|
#  |  0 | cobre       |   1994 | 3.63523e+06 |
#  
#  ------------------------------
#  