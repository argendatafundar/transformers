from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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
drop_col(col='pais', axis=1),
	wide_to_long(primary_keys=['code'], value_name='valor', var_name='serie'),
	rename_cols(map={'code': 'geocodigo'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   code    162 non-null    object 
#   1   pais    162 non-null    object 
#   2   gini    162 non-null    float64
#  
#  |    | code   | pais    |   gini |
#  |---:|:-------|:--------|-------:|
#  |  0 | ISL    | Iceland |   24.3 |
#  
#  ------------------------------
#  
#  drop_col(col='pais', axis=1)
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   code    162 non-null    object 
#   1   gini    162 non-null    float64
#  
#  |    | code   |   gini |
#  |---:|:-------|-------:|
#  |  0 | ISL    |   24.3 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['code'], value_name='valor', var_name='serie')
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   code    162 non-null    object 
#   1   serie   162 non-null    object 
#   2   valor   162 non-null    float64
#  
#  |    | code   | serie   |   valor |
#  |---:|:-------|:--------|--------:|
#  |  0 | ISL    | gini    |    24.3 |
#  
#  ------------------------------
#  
#  rename_cols(map={'code': 'geocodigo'})
#  RangeIndex: 162 entries, 0 to 161
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  162 non-null    object 
#   1   serie      162 non-null    object 
#   2   valor      162 non-null    float64
#  
#  |    | geocodigo   | serie   |   valor |
#  |---:|:------------|:--------|--------:|
#  |  0 | ISL         | gini    |    24.3 |
#  
#  ------------------------------
#  