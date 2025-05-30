from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='share_fob', k=100),
	replace_value(col='gran_rubro', curr_value='PP', new_value='Productos primarios'),
	replace_value(col='gran_rubro', curr_value='MOA', new_value='Manufacturas de origen agropecuario')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   gran_rubro       8 non-null      object 
#   1   producto         8 non-null      object 
#   2   toneladas        8 non-null      float64
#   3   fob_miles_usd    8 non-null      float64
#   4   share_toneladas  8 non-null      float64
#   5   share_fob        8 non-null      float64
#  
#  |    | gran_rubro   | producto                          |   toneladas |   fob_miles_usd |   share_toneladas |   share_fob |
#  |---:|:-------------|:----------------------------------|------------:|----------------:|------------------:|------------:|
#  |  0 | MOA          | Filetes y demás carnes de pescado |     79321.9 |          254036 |          0.152354 |     13.4415 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='share_fob', k=100)
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   gran_rubro       8 non-null      object 
#   1   producto         8 non-null      object 
#   2   toneladas        8 non-null      float64
#   3   fob_miles_usd    8 non-null      float64
#   4   share_toneladas  8 non-null      float64
#   5   share_fob        8 non-null      float64
#  
#  |    | gran_rubro   | producto                          |   toneladas |   fob_miles_usd |   share_toneladas |   share_fob |
#  |---:|:-------------|:----------------------------------|------------:|----------------:|------------------:|------------:|
#  |  0 | MOA          | Filetes y demás carnes de pescado |     79321.9 |          254036 |          0.152354 |     1344.15 |
#  
#  ------------------------------
#  
#  replace_value(col='gran_rubro', curr_value='PP', new_value='Productos primarios')
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   gran_rubro       8 non-null      object 
#   1   producto         8 non-null      object 
#   2   toneladas        8 non-null      float64
#   3   fob_miles_usd    8 non-null      float64
#   4   share_toneladas  8 non-null      float64
#   5   share_fob        8 non-null      float64
#  
#  |    | gran_rubro   | producto                          |   toneladas |   fob_miles_usd |   share_toneladas |   share_fob |
#  |---:|:-------------|:----------------------------------|------------:|----------------:|------------------:|------------:|
#  |  0 | MOA          | Filetes y demás carnes de pescado |     79321.9 |          254036 |          0.152354 |     1344.15 |
#  
#  ------------------------------
#  
#  replace_value(col='gran_rubro', curr_value='MOA', new_value='Manufacturas de origen agropecuario')
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   gran_rubro       8 non-null      object 
#   1   producto         8 non-null      object 
#   2   toneladas        8 non-null      float64
#   3   fob_miles_usd    8 non-null      float64
#   4   share_toneladas  8 non-null      float64
#   5   share_fob        8 non-null      float64
#  
#  |    | gran_rubro                          | producto                          |   toneladas |   fob_miles_usd |   share_toneladas |   share_fob |
#  |---:|:------------------------------------|:----------------------------------|------------:|----------------:|------------------:|------------:|
#  |  0 | Manufacturas de origen agropecuario | Filetes y demás carnes de pescado |     79321.9 |          254036 |          0.152354 |     1344.15 |
#  
#  ------------------------------
#  