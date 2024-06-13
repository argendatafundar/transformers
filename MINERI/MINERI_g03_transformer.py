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
rename_cols(map={'sector': 'indicador', 'exportaciones_sector_perc': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       140 non-null    int64  
#   1   sector                     140 non-null    object 
#   2   exportaciones_sector_perc  140 non-null    float64
#  
#  |    |   anio | sector      |   exportaciones_sector_perc |
#  |---:|-------:|:------------|----------------------------:|
#  |  0 |   1994 | metaliferos |                    0.170485 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador', 'exportaciones_sector_perc': 'valor'})
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       140 non-null    int64  
#   1   indicador  140 non-null    object 
#   2   valor      140 non-null    float64
#  
#  |    |   anio | indicador   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   1994 | metaliferos | 0.170485 |
#  
#  ------------------------------
#  