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
rename_cols(map={'variable': 'categoria', 'ano': 'anio'}),
	replace_value(col='categoria', curr_value='ingresohorrario', new_value='Ingreso horario'),
	replace_value(col='categoria', curr_value='ingresomensual', new_value='Ingreso mensual')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       64 non-null     int64  
#   1   variable  64 non-null     object 
#   2   valor     64 non-null     float64
#  
#  |    |   ano | variable        |   valor |
#  |---:|------:|:----------------|--------:|
#  |  0 |  1992 | ingresohorrario |     100 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'categoria', 'ano': 'anio'})
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       64 non-null     int64  
#   1   categoria  64 non-null     object 
#   2   valor      64 non-null     float64
#  
#  |    |   anio | categoria       |   valor |
#  |---:|-------:|:----------------|--------:|
#  |  0 |   1992 | ingresohorrario |     100 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ingresohorrario', new_value='Ingreso horario')
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       64 non-null     int64  
#   1   categoria  64 non-null     object 
#   2   valor      64 non-null     float64
#  
#  |    |   anio | categoria       |   valor |
#  |---:|-------:|:----------------|--------:|
#  |  0 |   1992 | Ingreso horario |     100 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ingresomensual', new_value='Ingreso mensual')
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       64 non-null     int64  
#   1   categoria  64 non-null     object 
#   2   valor      64 non-null     float64
#  
#  |    |   anio | categoria       |   valor |
#  |---:|-------:|:----------------|--------:|
#  |  0 |   1992 | Ingreso horario |     100 |
#  
#  ------------------------------
#  