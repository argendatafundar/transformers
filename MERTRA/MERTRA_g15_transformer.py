from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region_wvs_code  12 non-null     int64  
#   1   region_wvs       12 non-null     object 
#   2   nivel_acuerdo    12 non-null     object 
#   3   valor            12 non-null     float64
#  
#  |    |   region_wvs_code | region_wvs                     | nivel_acuerdo   |    valor |
#  |---:|------------------:|:-------------------------------|:----------------|---------:|
#  |  0 |                 1 | Centro y Sur (exc. CABA y PBA) | De acuerdo      | 0.128527 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region_wvs_code  12 non-null     int64  
#   1   region_wvs       12 non-null     object 
#   2   nivel_acuerdo    12 non-null     object 
#   3   valor            12 non-null     float64
#  
#  |    |   region_wvs_code | region_wvs                     | nivel_acuerdo   |   valor |
#  |---:|------------------:|:-------------------------------|:----------------|--------:|
#  |  0 |                 1 | Centro y Sur (exc. CABA y PBA) | De acuerdo      | 12.8527 |
#  
#  ------------------------------
#  