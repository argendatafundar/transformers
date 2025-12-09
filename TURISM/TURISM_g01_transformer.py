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
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   indicador      14 non-null     object 
#   1   anio           14 non-null     int64  
#   2   valor          14 non-null     float64
#   3   unidad_medida  14 non-null     object 
#  
#  |    | indicador                                      |   anio |     valor | unidad_medida       |
#  |---:|:-----------------------------------------------|-------:|----------:|:--------------------|
#  |  0 | Valor agregado bruto directo turístico (VABDT) |   2016 | 0.0177544 | en % sobre el total |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   indicador      14 non-null     object 
#   1   anio           14 non-null     int64  
#   2   valor          14 non-null     float64
#   3   unidad_medida  14 non-null     object 
#  
#  |    | indicador                                      |   anio |   valor | unidad_medida       |
#  |---:|:-----------------------------------------------|-------:|--------:|:--------------------|
#  |  0 | Valor agregado bruto directo turístico (VABDT) |   2016 | 1.77544 | en % sobre el total |
#  
#  ------------------------------
#  