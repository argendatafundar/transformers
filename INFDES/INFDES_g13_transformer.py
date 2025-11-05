from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def filtrar_por_max_anio(df, group_cols):
    idx = df.groupby(group_cols)['anio'].transform('max') == df['anio']
    return df[idx]
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	filtrar_por_max_anio(group_cols=['geonombreFundar', 'estado'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 193 entries, 0 to 192
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    193 non-null    object 
#   1   geonombreFundar    193 non-null    object 
#   2   anio               193 non-null    int64  
#   3   estado             193 non-null    object 
#   4   satisfaccion_vida  193 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | estado     |   satisfaccion_vida |
#  |---:|:------------------|:------------------|-------:|:-----------|--------------------:|
#  |  0 | ALB               | Albania           |   2018 | Desocupado |               6.642 |
#  
#  ------------------------------
#  
#  filtrar_por_max_anio(group_cols=['geonombreFundar', 'estado'])
#  Index: 179 entries, 0 to 192
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    179 non-null    object 
#   1   geonombreFundar    179 non-null    object 
#   2   anio               179 non-null    int64  
#   3   estado             179 non-null    object 
#   4   satisfaccion_vida  179 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | estado     |   satisfaccion_vida |
#  |---:|:------------------|:------------------|-------:|:-----------|--------------------:|
#  |  0 | ALB               | Albania           |   2018 | Desocupado |               6.642 |
#  
#  ------------------------------
#  