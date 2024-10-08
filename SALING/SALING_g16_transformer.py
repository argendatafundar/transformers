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
	replace_value(col='categoria', curr_value='Profesionales', new_value='Cuentapropistas profesionales'),
	replace_value(col='categoria', curr_value='Cuentapropistas', new_value='Cuentapropistas no profesionales'),
	replace_value(col='categoria', curr_value='Asalariadosfirmasgrandes', new_value='Asalariados firmas grandes'),
	replace_value(col='categoria', curr_value='Asalariadospequeñas', new_value='Asalariados firmas pequeñas'),
	replace_value(col='categoria', curr_value='Asalariadospúblico', new_value='Asalariados sector público')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 192 entries, 0 to 191
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype 
#  ---  ------    --------------  ----- 
#   0   ano       192 non-null    int64 
#   1   variable  192 non-null    object
#   2   valor     192 non-null    int64 
#  
#  |    |   ano | variable    |   valor |
#  |---:|------:|:------------|--------:|
#  |  0 |  1992 | Empresarios |     140 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'categoria', 'ano': 'anio'})
#  RangeIndex: 192 entries, 0 to 191
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       192 non-null    int64 
#   1   categoria  192 non-null    object
#   2   valor      192 non-null    int64 
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios |     140 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Profesionales', new_value='Cuentapropistas profesionales')
#  RangeIndex: 192 entries, 0 to 191
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       192 non-null    int64 
#   1   categoria  192 non-null    object
#   2   valor      192 non-null    int64 
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios |     140 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Cuentapropistas', new_value='Cuentapropistas no profesionales')
#  RangeIndex: 192 entries, 0 to 191
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       192 non-null    int64 
#   1   categoria  192 non-null    object
#   2   valor      192 non-null    int64 
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios |     140 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Asalariadosfirmasgrandes', new_value='Asalariados firmas grandes')
#  RangeIndex: 192 entries, 0 to 191
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       192 non-null    int64 
#   1   categoria  192 non-null    object
#   2   valor      192 non-null    int64 
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios |     140 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Asalariadospequeñas', new_value='Asalariados firmas pequeñas')
#  RangeIndex: 192 entries, 0 to 191
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       192 non-null    int64 
#   1   categoria  192 non-null    object
#   2   valor      192 non-null    int64 
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios |     140 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Asalariadospúblico', new_value='Asalariados sector público')
#  RangeIndex: 192 entries, 0 to 191
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       192 non-null    int64 
#   1   categoria  192 non-null    object
#   2   valor      192 non-null    int64 
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios |     140 |
#  
#  ------------------------------
#  