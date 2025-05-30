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
	multiplicar_por_escalar(col='valor_empalme', k=0.001)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 75 entries, 0 to 74
#  Data columns (total 4 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   anio                         75 non-null     int64  
#   1   desembarque_toneladas_fao    74 non-null     float64
#   2   desembarque_toneladas_magyp  36 non-null     float64
#   3   valor_empalme                75 non-null     float64
#  
#  |    |   anio |   desembarque_toneladas_fao |   desembarque_toneladas_magyp |   valor_empalme |
#  |---:|-------:|----------------------------:|------------------------------:|----------------:|
#  |  0 |   1950 |                       43900 |                           nan |         66251.2 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor_empalme', k=0.001)
#  RangeIndex: 75 entries, 0 to 74
#  Data columns (total 4 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   anio                         75 non-null     int64  
#   1   desembarque_toneladas_fao    74 non-null     float64
#   2   desembarque_toneladas_magyp  36 non-null     float64
#   3   valor_empalme                75 non-null     float64
#  
#  |    |   anio |   desembarque_toneladas_fao |   desembarque_toneladas_magyp |   valor_empalme |
#  |---:|-------:|----------------------------:|------------------------------:|----------------:|
#  |  0 |   1950 |                       43900 |                           nan |         66.2512 |
#  
#  ------------------------------
#  