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

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'ano': 'anio', 'variable': 'categoria'}),
	replace_value(col='categoria', curr_value='remuneraciontrabajo', new_value='Remuneración al trabajo asalariado'),
	replace_value(col='categoria', curr_value='ingresobrutomixto', new_value='Ingreso mixto bruto'),
	replace_value(col='categoria', curr_value='excedentebruto', new_value='Excedente de explotación bruto')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       21 non-null     int64  
#   1   variable  21 non-null     object 
#   2   valor     21 non-null     float64
#  
#  |    |   ano | variable            |   valor |
#  |---:|------:|:--------------------|--------:|
#  |  0 |  2016 | remuneraciontrabajo |      52 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'categoria'})
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       21 non-null     int64  
#   1   categoria  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    |   anio | categoria           |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   2016 | remuneraciontrabajo |      52 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='remuneraciontrabajo', new_value='Remuneración al trabajo asalariado')
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       21 non-null     int64  
#   1   categoria  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    |   anio | categoria                          |   valor |
#  |---:|-------:|:-----------------------------------|--------:|
#  |  0 |   2016 | Remuneración al trabajo asalariado |      52 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ingresobrutomixto', new_value='Ingreso mixto bruto')
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       21 non-null     int64  
#   1   categoria  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    |   anio | categoria                          |   valor |
#  |---:|-------:|:-----------------------------------|--------:|
#  |  0 |   2016 | Remuneración al trabajo asalariado |      52 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='excedentebruto', new_value='Excedente de explotación bruto')
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       21 non-null     int64  
#   1   categoria  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    |   anio | categoria                          |   valor |
#  |---:|-------:|:-----------------------------------|--------:|
#  |  0 |   2016 | Remuneración al trabajo asalariado |      52 |
#  
#  ------------------------------
#  