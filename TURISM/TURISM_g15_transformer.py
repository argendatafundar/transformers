from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='medida', replacements={'Arribos turísticos en temporada, en cantidad de personas': 'Verano', 'Arribos turísticos anuales en cantidad de personas': 'Total anual'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 91 entries, 0 to 90
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              91 non-null     int64  
#   1   medida            91 non-null     object 
#   2   arribos_turistas  91 non-null     float64
#   3   fuente            91 non-null     object 
#   4   tipo_serie        91 non-null     object 
#  
#  |    |   anio | medida                                                   |   arribos_turistas | fuente           | tipo_serie   |
#  |---:|-------:|:---------------------------------------------------------|-------------------:|:-----------------|:-------------|
#  |  0 |   1887 | Arribos turísticos en temporada, en cantidad de personas |               1416 | Pastoriza (2008) | original     |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='medida', replacements={'Arribos turísticos en temporada, en cantidad de personas': 'Verano', 'Arribos turísticos anuales en cantidad de personas': 'Total anual'})
#  RangeIndex: 91 entries, 0 to 90
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              91 non-null     int64  
#   1   medida            91 non-null     object 
#   2   arribos_turistas  91 non-null     float64
#   3   fuente            91 non-null     object 
#   4   tipo_serie        91 non-null     object 
#  
#  |    |   anio | medida   |   arribos_turistas | fuente           | tipo_serie   |
#  |---:|-------:|:---------|-------------------:|:-----------------|:-------------|
#  |  0 |   1887 | Verano   |               1416 | Pastoriza (2008) | original     |
#  
#  ------------------------------
#  