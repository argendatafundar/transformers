from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys=['anio', 'empleo_base'], value_name='valor', var_name='indicador'),
	rename_cols(map={'empleo_base': 'serie'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 81 entries, 0 to 80
#  Data columns (total 4 columns):
#   #   Column                           Non-Null Count  Dtype  
#  ---  ------                           --------------  -----  
#   0   anio                             81 non-null     int64  
#   1   empleo_minero_perc_formal_total  81 non-null     float64
#   2   empleo_base                      81 non-null     object 
#   3   cantidad_puestos                 81 non-null     float64
#  
#  |    |   anio |   empleo_minero_perc_formal_total | empleo_base       |   cantidad_puestos |
#  |---:|-------:|----------------------------------:|:------------------|-------------------:|
#  |  0 |   1996 |                          0.383496 | suma_mineria_oede |            10283.8 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'empleo_base'], value_name='valor', var_name='indicador')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         162 non-null    int64  
#   1   empleo_base  162 non-null    object 
#   2   indicador    162 non-null    object 
#   3   valor        162 non-null    float64
#  
#  |    |   anio | empleo_base       | indicador                       |    valor |
#  |---:|-------:|:------------------|:--------------------------------|---------:|
#  |  0 |   1996 | suma_mineria_oede | empleo_minero_perc_formal_total | 0.383496 |
#  
#  ------------------------------
#  
#  rename_cols(map={'empleo_base': 'serie'})
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       162 non-null    int64  
#   1   serie      162 non-null    object 
#   2   indicador  162 non-null    object 
#   3   valor      162 non-null    float64
#  
#  |    |   anio | serie             | indicador                       |    valor |
#  |---:|-------:|:------------------|:--------------------------------|---------:|
#  |  0 |   1996 | suma_mineria_oede | empleo_minero_perc_formal_total | 0.383496 |
#  
#  ------------------------------
#  