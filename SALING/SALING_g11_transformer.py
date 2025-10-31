from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'variable': 'categoria', 'ano': 'anio'}),
	replace_value(col='categoria', curr_value='ingresohorrario', new_value='Ingreso horario', mapping=None),
	replace_value(col='categoria', curr_value='ingresomensual', new_value='Ingreso mensual', mapping=None)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       66 non-null     int64  
#   1   variable  66 non-null     object 
#   2   valor     66 non-null     float64
#  
#  |    |   ano | variable        |   valor |
#  |---:|------:|:----------------|--------:|
#  |  0 |  1992 | ingresohorrario |     100 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'categoria', 'ano': 'anio'})
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       66 non-null     int64  
#   1   categoria  66 non-null     object 
#   2   valor      66 non-null     float64
#  
#  |    |   anio | categoria       |   valor |
#  |---:|-------:|:----------------|--------:|
#  |  0 |   1992 | ingresohorrario |     100 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ingresohorrario', new_value='Ingreso horario', mapping=None)
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       66 non-null     int64  
#   1   categoria  66 non-null     object 
#   2   valor      66 non-null     float64
#  
#  |    |   anio | categoria       |   valor |
#  |---:|-------:|:----------------|--------:|
#  |  0 |   1992 | Ingreso horario |     100 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='ingresomensual', new_value='Ingreso mensual', mapping=None)
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       66 non-null     int64  
#   1   categoria  66 non-null     object 
#   2   valor      66 non-null     float64
#  
#  |    |   anio | categoria       |   valor |
#  |---:|-------:|:----------------|--------:|
#  |  0 |   1992 | Ingreso horario |     100 |
#  
#  ------------------------------
#  