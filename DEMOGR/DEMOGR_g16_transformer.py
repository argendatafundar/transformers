from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='fuente', curr_value='World Population Prospects (UN)', new_value='WPP'),
	replace_value(col='fuente', curr_value='Lattes et al (1975)', new_value='Lattes et al. (1975)'),
	replace_value(col='fuente', curr_value='Maddison Project Database', new_value='MDP')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 94 entries, 0 to 93
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       94 non-null     int64 
#   1   poblacion  94 non-null     int64 
#   2   fuente     94 non-null     object
#  
#  |    |   anio |   poblacion | fuente                    |
#  |---:|-------:|------------:|:--------------------------|
#  |  0 |   1820 |      534000 | Maddison Project Database |
#  
#  ------------------------------
#  
#  replace_value(col='fuente', curr_value='World Population Prospects (UN)', new_value='WPP')
#  RangeIndex: 94 entries, 0 to 93
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       94 non-null     int64 
#   1   poblacion  94 non-null     int64 
#   2   fuente     94 non-null     object
#  
#  |    |   anio |   poblacion | fuente                    |
#  |---:|-------:|------------:|:--------------------------|
#  |  0 |   1820 |      534000 | Maddison Project Database |
#  
#  ------------------------------
#  
#  replace_value(col='fuente', curr_value='Lattes et al (1975)', new_value='Lattes et al. (1975)')
#  RangeIndex: 94 entries, 0 to 93
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       94 non-null     int64 
#   1   poblacion  94 non-null     int64 
#   2   fuente     94 non-null     object
#  
#  |    |   anio |   poblacion | fuente                    |
#  |---:|-------:|------------:|:--------------------------|
#  |  0 |   1820 |      534000 | Maddison Project Database |
#  
#  ------------------------------
#  
#  replace_value(col='fuente', curr_value='Maddison Project Database', new_value='MDP')
#  RangeIndex: 94 entries, 0 to 93
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       94 non-null     int64 
#   1   poblacion  94 non-null     int64 
#   2   fuente     94 non-null     object
#  
#  |    |   anio |   poblacion | fuente   |
#  |---:|-------:|------------:|:---------|
#  |  0 |   1820 |      534000 | MDP      |
#  
#  ------------------------------
#  