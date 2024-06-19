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
wide_to_long(primary_keys=['sector', 'anio', 'subsector'], value_name='valor', var_name='indicador'),
	rename_cols(map={'sector': 'nivel1', 'subsector': 'nivel2'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   sector            15 non-null     object 
#   1   anio              15 non-null     int64  
#   2   subsector         15 non-null     object 
#   3   valor_en_mtco2e   15 non-null     float64
#   4   valor_en_porcent  15 non-null     float64
#  
#  |    | sector   |   anio | subsector                |   valor_en_mtco2e |   valor_en_porcent |
#  |---:|:---------|-------:|:-------------------------|------------------:|-------------------:|
#  |  0 | Energía  |   2018 | Industrias de la energía |            58.627 |            16.0231 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['sector', 'anio', 'subsector'], value_name='valor', var_name='indicador')
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   sector     30 non-null     object 
#   1   anio       30 non-null     int64  
#   2   subsector  30 non-null     object 
#   3   indicador  30 non-null     object 
#   4   valor      30 non-null     float64
#  
#  |    | sector   |   anio | subsector                | indicador       |   valor |
#  |---:|:---------|-------:|:-------------------------|:----------------|--------:|
#  |  0 | Energía  |   2018 | Industrias de la energía | valor_en_mtco2e |  58.627 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'nivel1', 'subsector': 'nivel2'})
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   nivel1     30 non-null     object 
#   1   anio       30 non-null     int64  
#   2   nivel2     30 non-null     object 
#   3   indicador  30 non-null     object 
#   4   valor      30 non-null     float64
#  
#  |    | nivel1   |   anio | nivel2                   | indicador       |   valor |
#  |---:|:---------|-------:|:-------------------------|:----------------|--------:|
#  |  0 | Energía  |   2018 | Industrias de la energía | valor_en_mtco2e |  58.627 |
#  
#  ------------------------------
#  