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
	replace_value(col='categoria', curr_value='Profesionales', new_value='Cuentapropistas profesionales', mapping=None),
	replace_value(col='categoria', curr_value='Cuentapropistas', new_value='Cuentapropistas no profesionales', mapping=None),
	replace_value(col='categoria', curr_value='Asalariadosfirmasgrandes', new_value='Asalariados firmas grandes', mapping=None),
	replace_value(col='categoria', curr_value='Asalariadospequeñas', new_value='Asalariados firmas pequeñas', mapping=None),
	replace_value(col='categoria', curr_value='Asalariadospúblico', new_value='Asalariados sector público', mapping=None)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 198 entries, 0 to 197
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       198 non-null    int64  
#   1   variable  198 non-null    object 
#   2   valor     198 non-null    float64
#  
#  |    |   ano | variable    |   valor |
#  |---:|------:|:------------|--------:|
#  |  0 |  1992 | Empresarios | 31832.3 |
#  
#  ------------------------------
#  
#  rename_cols(map={'variable': 'categoria', 'ano': 'anio'})
#  RangeIndex: 198 entries, 0 to 197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       198 non-null    int64  
#   1   categoria  198 non-null    object 
#   2   valor      198 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios | 31832.3 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Profesionales', new_value='Cuentapropistas profesionales', mapping=None)
#  RangeIndex: 198 entries, 0 to 197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       198 non-null    int64  
#   1   categoria  198 non-null    object 
#   2   valor      198 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios | 31832.3 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Cuentapropistas', new_value='Cuentapropistas no profesionales', mapping=None)
#  RangeIndex: 198 entries, 0 to 197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       198 non-null    int64  
#   1   categoria  198 non-null    object 
#   2   valor      198 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios | 31832.3 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Asalariadosfirmasgrandes', new_value='Asalariados firmas grandes', mapping=None)
#  RangeIndex: 198 entries, 0 to 197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       198 non-null    int64  
#   1   categoria  198 non-null    object 
#   2   valor      198 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios | 31832.3 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Asalariadospequeñas', new_value='Asalariados firmas pequeñas', mapping=None)
#  RangeIndex: 198 entries, 0 to 197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       198 non-null    int64  
#   1   categoria  198 non-null    object 
#   2   valor      198 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios | 31832.3 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Asalariadospúblico', new_value='Asalariados sector público', mapping=None)
#  RangeIndex: 198 entries, 0 to 197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       198 non-null    int64  
#   1   categoria  198 non-null    object 
#   2   valor      198 non-null    float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Empresarios | 31832.3 |
#  
#  ------------------------------
#  