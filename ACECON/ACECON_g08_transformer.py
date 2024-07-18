from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'remuneracion_al_trabajo_asalariado': 'Masa Salarial', 'otros': 'Resto de los Ingresos'}),
	wide_to_long(primary_keys=['anio'], value_name='valor', var_name='indicador'),
	sort_values(how='ascending', by=['anio'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 3 columns):
#   #   Column                              Non-Null Count  Dtype  
#  ---  ------                              --------------  -----  
#   0   anio                                89 non-null     int64  
#   1   remuneracion_al_trabajo_asalariado  89 non-null     float64
#   2   otros                               89 non-null     float64
#  
#  |    |   anio |   remuneracion_al_trabajo_asalariado |   otros |
#  |---:|-------:|-------------------------------------:|--------:|
#  |  0 |   1935 |                               35.294 |  64.706 |
#  
#  ------------------------------
#  
#  rename_cols(map={'remuneracion_al_trabajo_asalariado': 'Masa Salarial', 'otros': 'Resto de los Ingresos'})
#  RangeIndex: 89 entries, 0 to 88
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   89 non-null     int64  
#   1   Masa Salarial          89 non-null     float64
#   2   Resto de los Ingresos  89 non-null     float64
#  
#  |    |   anio |   Masa Salarial |   Resto de los Ingresos |
#  |---:|-------:|----------------:|------------------------:|
#  |  0 |   1935 |          35.294 |                  64.706 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio'], value_name='valor', var_name='indicador')
#  RangeIndex: 178 entries, 0 to 177
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       178 non-null    int64  
#   1   indicador  178 non-null    object 
#   2   valor      178 non-null    float64
#  
#  |    |   anio | indicador     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1935 | Masa Salarial |  35.294 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio'])
#  RangeIndex: 178 entries, 0 to 177
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       178 non-null    int64  
#   1   indicador  178 non-null    object 
#   2   valor      178 non-null    float64
#  
#  |    |   anio | indicador     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1935 | Masa Salarial |  35.294 |
#  
#  ------------------------------
#  