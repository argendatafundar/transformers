from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys=['anio'], value_name='valor', var_name='indicador')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              88 non-null     int64  
#   1   petroleo_mineria  88 non-null     float64
#   2   mineria           19 non-null     float64
#  
#  |    |   anio |   petroleo_mineria |   mineria |
#  |---:|-------:|-------------------:|----------:|
#  |  0 |   1935 |           0.380483 |       nan |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio'], value_name='valor', var_name='indicador')
#  RangeIndex: 176 entries, 0 to 175
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       176 non-null    int64  
#   1   indicador  176 non-null    object 
#   2   valor      107 non-null    float64
#  
#  |    |   anio | indicador        |    valor |
#  |---:|-------:|:-----------------|---------:|
#  |  0 |   1935 | petroleo_mineria | 0.380483 |
#  
#  ------------------------------
#  