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
rename_columns(sector='categoria', estado_ocupacional='indicador', prop_ocupados='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   sector              15 non-null     object 
#   1   estado_ocupacional  15 non-null     object 
#   2   prop_ocupados       15 non-null     float64
#  
#  |    | sector                                               | estado_ocupacional       |   prop_ocupados |
#  |---:|:-----------------------------------------------------|:-------------------------|----------------:|
#  |  0 | a. Actividades profesionales, cientificas y técnicas | Asalariado no registrado |        0.146243 |
#  
#  ------------------------------
#  
#  rename_columns(sector='categoria', estado_ocupacional='indicador', prop_ocupados='valor')
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  15 non-null     object 
#   1   indicador  15 non-null     object 
#   2   valor      15 non-null     float64
#  
#  |    | categoria                                            | indicador                |    valor |
#  |---:|:-----------------------------------------------------|:-------------------------|---------:|
#  |  0 | a. Actividades profesionales, cientificas y técnicas | Asalariado no registrado | 0.146243 |
#  
#  ------------------------------
#  