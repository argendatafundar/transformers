from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: pl.DataFrame) -> pl.DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 6 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   tipo           30 non-null     object 
#   1   anio           30 non-null     int64  
#   2   destino        30 non-null     object 
#   3   inversion_i_d  30 non-null     float64
#   4   unidad_medida  30 non-null     object 
#   5   share          30 non-null     float64
#  
#  |    | tipo     |   anio | destino                 |   inversion_i_d | unidad_medida                |   share |
#  |---:|:---------|-------:|:------------------------|----------------:|:-----------------------------|--------:|
#  |  0 | Empresas |   2023 | Erogaciones en personal |          255384 | millones de pesos corrientes | 52.7628 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 6 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   tipo           30 non-null     object 
#   1   anio           30 non-null     int64  
#   2   destino        30 non-null     object 
#   3   inversion_i_d  30 non-null     float64
#   4   unidad_medida  30 non-null     object 
#   5   share          30 non-null     float64
#  
#  |    | tipo     |   anio | destino                 |   inversion_i_d | unidad_medida                |   share |
#  |---:|:---------|-------:|:------------------------|----------------:|:-----------------------------|--------:|
#  |  0 | Empresas |   2023 | Erogaciones en personal |          255384 | millones de pesos corrientes | 52.7628 |
#  
#  ------------------------------
#  