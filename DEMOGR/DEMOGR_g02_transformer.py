from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio <= 2025'),
	replace_value(col='fuente', curr_value='World Population Prospects (UN)', new_value='WPP')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 156 entries, 0 to 155
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          156 non-null    int64  
#   1   edad_mediana  155 non-null    float64
#   2   fuente        156 non-null    object 
#  
#  |    |   anio |   edad_mediana | fuente   |
#  |---:|-------:|---------------:|:---------|
#  |  0 |   1869 |        16.5049 | INDEC    |
#  
#  ------------------------------
#  
#  query(condition='anio <= 2025')
#  Index: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          80 non-null     int64  
#   1   edad_mediana  80 non-null     float64
#   2   fuente        80 non-null     object 
#  
#  |    |   anio |   edad_mediana | fuente   |
#  |---:|-------:|---------------:|:---------|
#  |  0 |   1869 |        16.5049 | INDEC    |
#  
#  ------------------------------
#  
#  replace_value(col='fuente', curr_value='World Population Prospects (UN)', new_value='WPP')
#  Index: 80 entries, 0 to 79
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          80 non-null     int64  
#   1   edad_mediana  80 non-null     float64
#   2   fuente        80 non-null     object 
#  
#  |    |   anio |   edad_mediana | fuente   |
#  |---:|-------:|---------------:|:---------|
#  |  0 |   1869 |        16.5049 | INDEC    |
#  
#  ------------------------------
#  