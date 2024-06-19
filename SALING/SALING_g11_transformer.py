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
rename_cols(map={'variable': 'indicador', '            ano': 'anio'}),
	replace_value(col='indicador', curr_value='ingresohorrario', new_value='Ingreso laboral por hora'),
	replace_value(col='indicador', curr_value='ingresomensual', new_value='Ingreso laboral por mes')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0               ano  12 non-null     object 
#   1   variable         10 non-null     object 
#   2   valor            10 non-null     float64
#  
#  |    |             ano   | variable        |   valor |
#  |---:|:------------------|:----------------|--------:|
#  |  0 | 0     1992        | ingresohorrario |     100 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'indicador', '            ano': 'anio'})
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       12 non-null     object 
#   1   indicador  10 non-null     object 
#   2   valor      10 non-null     float64
#  
#  |    | anio       | indicador       |   valor |
#  |---:|:-----------|:----------------|--------:|
#  |  0 | 0     1992 | ingresohorrario |     100 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='ingresohorrario', new_value='Ingreso laboral por hora')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       12 non-null     object 
#   1   indicador  10 non-null     object 
#   2   valor      10 non-null     float64
#  
#  |    | anio       | indicador                |   valor |
#  |---:|:-----------|:-------------------------|--------:|
#  |  0 | 0     1992 | Ingreso laboral por hora |     100 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='ingresomensual', new_value='Ingreso laboral por mes')
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       12 non-null     object 
#   1   indicador  10 non-null     object 
#   2   valor      10 non-null     float64
#  
#  |    |   anio | indicador                |   valor |
#  |---:|-------:|:-------------------------|--------:|
#  |  0 |   1992 | Ingreso laboral por hora |     100 |
#  
#  ------------------------------
#  