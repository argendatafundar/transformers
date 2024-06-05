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
wide_to_long(primary_keys=['anio', 'grupo_nuevo'], value_name='valor', var_name='indicador'),
	rename_cols(map={'grupo_nuevo': 'serie'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 139 entries, 0 to 138
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   grupo_nuevo  139 non-null    object 
#   1   anio         139 non-null    int64  
#   2   expo_grupo   139 non-null    float64
#  
#  |    | grupo_nuevo   |   anio |   expo_grupo |
#  |---:|:--------------|-------:|-------------:|
#  |  0 | cobre         |   1994 |  3.63523e+06 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'grupo_nuevo'], value_name='valor', var_name='indicador')
#  RangeIndex: 139 entries, 0 to 138
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         139 non-null    int64  
#   1   grupo_nuevo  139 non-null    object 
#   2   indicador    139 non-null    object 
#   3   valor        139 non-null    float64
#  
#  |    |   anio | grupo_nuevo   | indicador   |       valor |
#  |---:|-------:|:--------------|:------------|------------:|
#  |  0 |   1994 | cobre         | expo_grupo  | 3.63523e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'grupo_nuevo': 'serie'})
#  RangeIndex: 139 entries, 0 to 138
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       139 non-null    int64  
#   1   serie      139 non-null    object 
#   2   indicador  139 non-null    object 
#   3   valor      139 non-null    float64
#  
#  |    |   anio | serie   | indicador   |       valor |
#  |---:|-------:|:--------|:------------|------------:|
#  |  0 |   1994 | cobre   | expo_grupo  | 3.63523e+06 |
#  
#  ------------------------------
#  