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
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   grupo_nuevo  145 non-null    object 
#   1   anio         145 non-null    int64  
#   2   impo_grupo   145 non-null    float64
#  
#  |    | grupo_nuevo   |   anio |   impo_grupo |
#  |---:|:--------------|-------:|-------------:|
#  |  0 | aluminio      |   1994 |  8.39544e+06 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'grupo_nuevo'], value_name='valor', var_name='indicador')
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 4 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         145 non-null    int64  
#   1   grupo_nuevo  145 non-null    object 
#   2   indicador    145 non-null    object 
#   3   valor        145 non-null    float64
#  
#  |    |   anio | grupo_nuevo   | indicador   |       valor |
#  |---:|-------:|:--------------|:------------|------------:|
#  |  0 |   1994 | aluminio      | impo_grupo  | 8.39544e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'grupo_nuevo': 'serie'})
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       145 non-null    int64  
#   1   serie      145 non-null    object 
#   2   indicador  145 non-null    object 
#   3   valor      145 non-null    float64
#  
#  |    |   anio | serie    | indicador   |       valor |
#  |---:|-------:|:---------|:------------|------------:|
#  |  0 |   1994 | aluminio | impo_grupo  | 8.39544e+06 |
#  
#  ------------------------------
#  