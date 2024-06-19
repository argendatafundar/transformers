from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'country_code': 'categoria', 'year': 'anio', 'ipcf_promedio': 'valor'}),
	replace_value(col='categoria', curr_value='ARG', new_value='Argentina'),
	replace_value(col='categoria', curr_value='LAC', new_value='América Latina y el Caribe (excluidos Países de Altos Ingresos)')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   country_code   60 non-null     object 
#   1   year           60 non-null     int64  
#   2   ipcf_promedio  60 non-null     float64
#  
#  |    | country_code   |   year |   ipcf_promedio |
#  |---:|:---------------|-------:|----------------:|
#  |  0 | ARG            |   1992 |         699.336 |
#  
#  ------------------------------
#  
#  rename_cols(map={'country_code': 'categoria', 'year': 'anio', 'ipcf_promedio': 'valor'})
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   anio       60 non-null     int64  
#   2   valor      60 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | ARG         |   1992 | 699.336 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ARG', new_value='Argentina')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   anio       60 non-null     int64  
#   2   valor      60 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   1992 | 699.336 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='LAC', new_value='América Latina y el Caribe (excluidos Países de Altos Ingresos)')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  60 non-null     object 
#   1   anio       60 non-null     int64  
#   2   valor      60 non-null     float64
#  
#  |    | categoria   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Argentina   |   1992 | 699.336 |
#  
#  ------------------------------
#  