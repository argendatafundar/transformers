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
wide_to_long(primary_keys=['anio', 'sector'], value_name='valor', var_name='indicador'),
	rename_cols(map={'sector': 'serie'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 3 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   anio                       140 non-null    int64  
#   1   sector                     140 non-null    object 
#   2   exportaciones_sector_perc  140 non-null    float64
#  
#  |    |   anio | sector      |   exportaciones_sector_perc |
#  |---:|-------:|:------------|----------------------------:|
#  |  0 |   1994 | metaliferos |                    0.170485 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'sector'], value_name='valor', var_name='indicador')
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       140 non-null    int64  
#   1   sector     140 non-null    object 
#   2   indicador  140 non-null    object 
#   3   valor      140 non-null    float64
#  
#  |    |   anio | sector      | indicador                 |    valor |
#  |---:|-------:|:------------|:--------------------------|---------:|
#  |  0 |   1994 | metaliferos | exportaciones_sector_perc | 0.170485 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'serie'})
#  RangeIndex: 140 entries, 0 to 139
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       140 non-null    int64  
#   1   serie      140 non-null    object 
#   2   indicador  140 non-null    object 
#   3   valor      140 non-null    float64
#  
#  |    |   anio | serie       | indicador                 |    valor |
#  |---:|-------:|:------------|:--------------------------|---------:|
#  |  0 |   1994 | metaliferos | exportaciones_sector_perc | 0.170485 |
#  
#  ------------------------------
#  