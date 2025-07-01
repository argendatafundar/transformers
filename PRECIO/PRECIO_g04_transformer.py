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
	rename_columns(geocodigoFundar='geocodigo', geonombreFundar='geonombre', inflacion_prom_07_22='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 137 entries, 0 to 136
#  Data columns (total 3 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geocodigoFundar       137 non-null    object 
#   1   geonombreFundar       137 non-null    object 
#   2   inflacion_prom_07_22  137 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   inflacion_prom_07_22 |
#  |---:|:------------------|:------------------|-----------------------:|
#  |  0 | AFG               | Afganistán        |                 5.8615 |
#  
#  ------------------------------
#  
#  rename_columns(geocodigoFundar='geocodigo', geonombreFundar='geonombre', inflacion_prom_07_22='valor')
#  RangeIndex: 137 entries, 0 to 136
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  137 non-null    object 
#   1   geonombre  137 non-null    object 
#   2   valor      137 non-null    float64
#  
#  |    | geocodigo   | geonombre   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AFG         | Afganistán  |  5.8615 |
#  
#  ------------------------------
#  