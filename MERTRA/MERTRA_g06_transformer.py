from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

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
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='tasa_actividad', k=100),
	query(condition='anio == anio.max()'),
	replace_value(col='reg_desc_fundar', curr_value='Todas', new_value='Nacional', mapping=None)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 5184 entries, 0 to 5183
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             5184 non-null   int64  
#   1   edad             5184 non-null   int64  
#   2   reg_desc_fundar  5184 non-null   object 
#   3   tasa_actividad   5184 non-null   float64
#  
#  |    |   anio |   edad | reg_desc_fundar   |   tasa_actividad |
#  |---:|-------:|-------:|:------------------|-----------------:|
#  |  0 |   2016 |     10 | CABA              |                0 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='tasa_actividad', k=100)
#  RangeIndex: 5184 entries, 0 to 5183
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             5184 non-null   int64  
#   1   edad             5184 non-null   int64  
#   2   reg_desc_fundar  5184 non-null   object 
#   3   tasa_actividad   5184 non-null   float64
#  
#  |    |   anio |   edad | reg_desc_fundar   |   tasa_actividad |
#  |---:|-------:|-------:|:------------------|-----------------:|
#  |  0 |   2016 |     10 | CABA              |                0 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 648 entries, 4536 to 5183
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             648 non-null    int64  
#   1   edad             648 non-null    int64  
#   2   reg_desc_fundar  648 non-null    object 
#   3   tasa_actividad   648 non-null    float64
#  
#  |      |   anio |   edad | reg_desc_fundar   |   tasa_actividad |
#  |-----:|-------:|-------:|:------------------|-----------------:|
#  | 4536 |   2023 |     10 | CABA              |                0 |
#  
#  ------------------------------
#  
#  replace_value(col='reg_desc_fundar', curr_value='Todas', new_value='Nacional', mapping=None)
#  Index: 648 entries, 4536 to 5183
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             648 non-null    int64  
#   1   edad             648 non-null    int64  
#   2   reg_desc_fundar  648 non-null    object 
#   3   tasa_actividad   648 non-null    float64
#  
#  |      |   anio |   edad | reg_desc_fundar   |   tasa_actividad |
#  |-----:|-------:|-------:|:------------------|-----------------:|
#  | 4536 |   2023 |     10 | CABA              |                0 |
#  
#  ------------------------------
#  