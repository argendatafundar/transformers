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
	replace_value(col='industria_turistica', curr_value='Transporte aéreo de pasajeros', new_value='Vuelos', mapping=None),
	replace_value(col='industria_turistica', curr_value='Media industrias turísticas', new_value='Media ind. turísticas', mapping=None),
	replace_value(col='industria_turistica', curr_value='Otros productos no característicos', new_value='Otros productos', mapping=None)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 2 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   industria_turistica  8 non-null      object 
#   1   ratio                8 non-null      float64
#  
#  |    | industria_turistica         |   ratio |
#  |---:|:----------------------------|--------:|
#  |  0 | Media industrias turísticas | 30.2705 |
#  
#  ------------------------------
#  
#  replace_value(col='industria_turistica', curr_value='Transporte aéreo de pasajeros', new_value='Vuelos', mapping=None)
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 2 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   industria_turistica  8 non-null      object 
#   1   ratio                8 non-null      float64
#  
#  |    | industria_turistica         |   ratio |
#  |---:|:----------------------------|--------:|
#  |  0 | Media industrias turísticas | 30.2705 |
#  
#  ------------------------------
#  
#  replace_value(col='industria_turistica', curr_value='Media industrias turísticas', new_value='Media ind. turísticas', mapping=None)
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 2 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   industria_turistica  8 non-null      object 
#   1   ratio                8 non-null      float64
#  
#  |    | industria_turistica   |   ratio |
#  |---:|:----------------------|--------:|
#  |  0 | Media ind. turísticas | 30.2705 |
#  
#  ------------------------------
#  
#  replace_value(col='industria_turistica', curr_value='Otros productos no característicos', new_value='Otros productos', mapping=None)
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 2 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   industria_turistica  8 non-null      object 
#   1   ratio                8 non-null      float64
#  
#  |    | industria_turistica   |   ratio |
#  |---:|:----------------------|--------:|
#  |  0 | Media ind. turísticas | 30.2705 |
#  
#  ------------------------------
#  