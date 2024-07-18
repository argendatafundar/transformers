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
rename_cols(map={'comp_dem_ag': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 528 entries, 0 to 527
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         528 non-null    int64  
#   1   comp_dem_ag  528 non-null    object 
#   2   valor        528 non-null    float64
#  
#  |    |   anio | comp_dem_ag     |   valor |
#  |---:|-------:|:----------------|--------:|
#  |  0 |   1935 | Consumo hogares | 75.7468 |
#  
#  ------------------------------
#  
#  rename_cols(map={'comp_dem_ag': 'indicador'})
#  RangeIndex: 528 entries, 0 to 527
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       528 non-null    int64  
#   1   indicador  528 non-null    object 
#   2   valor      528 non-null    float64
#  
#  |    |   anio | indicador       |   valor |
#  |---:|-------:|:----------------|--------:|
#  |  0 |   1935 | Consumo hogares | 75.7468 |
#  
#  ------------------------------
#  