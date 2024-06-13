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
rename_cols(map={'rama_actividad': 'categoria', 'categoria_ocupacional': 'indicador', 'porcentaje_sobre_total_rama': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   rama_actividad               69 non-null     object 
#   1   categoria_ocupacional        69 non-null     object 
#   2   porcentaje_sobre_total_rama  69 non-null     float64
#  
#  |    | rama_actividad   | categoria_ocupacional   |   porcentaje_sobre_total_rama |
#  |---:|:-----------------|:------------------------|------------------------------:|
#  |  0 | Construcción     | asalariados_registrados |                         15.91 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rama_actividad': 'categoria', 'categoria_ocupacional': 'indicador', 'porcentaje_sobre_total_rama': 'valor'})
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  69 non-null     object 
#   1   indicador  69 non-null     object 
#   2   valor      69 non-null     float64
#  
#  |    | categoria    | indicador               |   valor |
#  |---:|:-------------|:------------------------|--------:|
#  |  0 | Construcción | asalariados_registrados |   15.91 |
#  
#  ------------------------------
#  