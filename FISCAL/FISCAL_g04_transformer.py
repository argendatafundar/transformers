from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: DataFrame) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 75 entries, 0 to 74
#  Data columns (total 4 columns):
#   #   Column                         Non-Null Count  Dtype  
#  ---  ------                         --------------  -----  
#   0   geocodigoFundar                75 non-null     object 
#   1   geonombreFundar                75 non-null     object 
#   2   tipo_pais                      75 non-null     object 
#   3   correlacion_ciclicidad_fiscal  75 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | tipo_pais              |   correlacion_ciclicidad_fiscal |
#  |---:|:------------------|:------------------|:-----------------------|--------------------------------:|
#  |  0 | ALB               | Albania           | Economía en desarrollo |                        0.439029 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 75 entries, 0 to 74
#  Data columns (total 4 columns):
#   #   Column                         Non-Null Count  Dtype  
#  ---  ------                         --------------  -----  
#   0   geocodigoFundar                75 non-null     object 
#   1   geonombreFundar                75 non-null     object 
#   2   tipo_pais                      75 non-null     object 
#   3   correlacion_ciclicidad_fiscal  75 non-null     float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | tipo_pais              |   correlacion_ciclicidad_fiscal |
#  |---:|:------------------|:------------------|:-----------------------|--------------------------------:|
#  |  0 | ALB               | Albania           | Economía en desarrollo |                        0.439029 |
#  
#  ------------------------------
#  