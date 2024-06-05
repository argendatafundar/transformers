from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys=['rama_actividad', 'categoria_ocupacional'], value_name='valor', var_name='indicador')
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
#  wide_to_long(primary_keys=['rama_actividad', 'categoria_ocupacional'], value_name='valor', var_name='indicador')
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 4 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   rama_actividad         69 non-null     object 
#   1   categoria_ocupacional  69 non-null     object 
#   2   indicador              69 non-null     object 
#   3   valor                  69 non-null     float64
#  
#  |    | rama_actividad   | categoria_ocupacional   | indicador                   |   valor |
#  |---:|:-----------------|:------------------------|:----------------------------|--------:|
#  |  0 | Construcción     | asalariados_registrados | porcentaje_sobre_total_rama |   15.91 |
#  
#  ------------------------------
#  