from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(iso3='geocodigo', inflacion_prom_07_22='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 2 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   iso3                  132 non-null    object 
#   1   inflacion_prom_07_22  132 non-null    float64
#  
#  |    | iso3   |   inflacion_prom_07_22 |
#  |---:|:-------|-----------------------:|
#  |  0 | AGO    |                16.9548 |
#  
#  ------------------------------
#  
#  rename_columns(iso3='geocodigo', inflacion_prom_07_22='valor')
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  132 non-null    object 
#   1   valor      132 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  |  0 | AGO         | 16.9548 |
#  
#  ------------------------------
#  