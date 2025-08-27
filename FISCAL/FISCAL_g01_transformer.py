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
	replace_value(col='geonombreFundar', curr_value='Estados Unidos', new_value='EE.UU.', mapping=None)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 151 entries, 0 to 150
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         151 non-null    object 
#   1   geonombreFundar         151 non-null    object 
#   2   gasto_publico_promedio  151 non-null    float64
#   3   fuente                  151 non-null    object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   gasto_publico_promedio | fuente   |
#  |---:|:------------------|:------------------|-------------------------:|:---------|
#  |  0 | KIR               | Kiribati          |                  97.4885 | FMI      |
#  
#  ------------------------------
#  
#  replace_value(col='geonombreFundar', curr_value='Estados Unidos', new_value='EE.UU.', mapping=None)
#  RangeIndex: 151 entries, 0 to 150
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         151 non-null    object 
#   1   geonombreFundar         151 non-null    object 
#   2   gasto_publico_promedio  151 non-null    float64
#   3   fuente                  151 non-null    object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   gasto_publico_promedio | fuente   |
#  |---:|:------------------|:------------------|-------------------------:|:---------|
#  |  0 | KIR               | Kiribati          |                  97.4885 | FMI      |
#  
#  ------------------------------
#  