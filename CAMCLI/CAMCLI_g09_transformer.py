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
rename_cols(map={'valor_en_mtco2e': 'valor', 'sector': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 116 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             116 non-null    int64  
#   1   sector           116 non-null    object 
#   2   valor_en_mtco2e  116 non-null    float64
#  
#  |    |   anio | sector   |   valor_en_mtco2e |
#  |---:|-------:|:---------|------------------:|
#  |  0 |   1990 | AGSyOUT  |            151.97 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_mtco2e': 'valor', 'sector': 'indicador'})
#  RangeIndex: 116 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       116 non-null    int64  
#   1   indicador  116 non-null    object 
#   2   valor      116 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1990 | AGSyOUT     |  151.97 |
#  
#  ------------------------------
#  