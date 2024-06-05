from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='variable', curr_value='remuneraciontrabajo', new_value='Remuneración al trabajo'),
	replace_value(col='variable', curr_value='ingresobrutomixto', new_value='Ingreso Bruto Mixto'),
	replace_value(col='variable', curr_value='excedentebruto', new_value='Excedente Bruto de Explotación'),
	rename_cols(map={'ano': 'anio', 'variable': 'indicador'})
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
#  replace_value(col='variable', curr_value='remuneraciontrabajo', new_value='Remuneración al trabajo')
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       21 non-null     int64  
#   1   variable  21 non-null     object 
#   2   valor     21 non-null     float64
#  
#  |    |   ano | variable                |   valor |
#  |---:|------:|:------------------------|--------:|
#  |  0 |  2016 | Remuneración al trabajo |      52 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='ingresobrutomixto', new_value='Ingreso Bruto Mixto')
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       21 non-null     int64  
#   1   variable  21 non-null     object 
#   2   valor     21 non-null     float64
#  
#  |    |   ano | variable                |   valor |
#  |---:|------:|:------------------------|--------:|
#  |  0 |  2016 | Remuneración al trabajo |      52 |
#  
#  ------------------------------
#  
#  replace_value(col='variable', curr_value='excedentebruto', new_value='Excedente Bruto de Explotación')
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       21 non-null     int64  
#   1   variable  21 non-null     object 
#   2   valor     21 non-null     float64
#  
#  |    |   ano | variable                |   valor |
#  |---:|------:|:------------------------|--------:|
#  |  0 |  2016 | Remuneración al trabajo |      52 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'indicador'})
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       21 non-null     int64  
#   1   indicador  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    |   anio | indicador               |   valor |
#  |---:|-------:|:------------------------|--------:|
#  |  0 |   2016 | Remuneración al trabajo |      52 |
#  
#  ------------------------------
#  