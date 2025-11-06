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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='sexo', curr_value=None, new_value=None, mapping={'Total': 'Totales', 'Mujeres': 'Mujer', 'Varones': 'Varon'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 9 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   sexo          9 non-null      object 
#   1   tipo_trabajo  9 non-null      object 
#   2   minutos       9 non-null      float64
#  
#  |    | sexo   | tipo_trabajo   |   minutos |
#  |---:|:-------|:---------------|----------:|
#  |  0 | Total  | Trabajo total  |   501.643 |
#  
#  ------------------------------
#  
#  replace_value(col='sexo', curr_value=None, new_value=None, mapping={'Total': 'Totales', 'Mujeres': 'Mujer', 'Varones': 'Varon'})
#  RangeIndex: 9 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   sexo          9 non-null      object 
#   1   tipo_trabajo  9 non-null      object 
#   2   minutos       9 non-null      float64
#  
#  |    | sexo    | tipo_trabajo   |   minutos |
#  |---:|:--------|:---------------|----------:|
#  |  0 | Totales | Trabajo total  |   501.643 |
#  
#  ------------------------------
#  