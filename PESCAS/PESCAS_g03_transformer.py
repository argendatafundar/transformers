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

@transformer.convert
def txtwrapper(df: DataFrame, col:str, width:int)->DataFrame:
    import textwrap
    df[col] = df[col].apply(lambda text: "\n".join(textwrap.wrap(text, width=width)))
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='share_fob', k=100),
	replace_value(col='producto', curr_value='Pescado sec./sal./en salm. har./pol./pell. aptos p/c humano', new_value='Otras preparaciones aptas para consumo humano'),
	replace_value(col='producto', curr_value='Harina, polvo y pellets de pescado. No aptos p/c humano', new_value='Harina, polvo y pellets de pescado. No aptos para consumo humano'),
	txtwrapper(col='producto', width=25)
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
#  |  0 | MOA          | Filetes y demás carnes de pescado |     79321.9 |          254036 |          0.152354 |    0.134415 |
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
#  |  0 | MOA          | Filetes y demás carnes de pescado |     79321.9 |          254036 |          0.152354 |     13.4415 |
#  
#  ------------------------------
#  
#  replace_value(col='producto', curr_value='Pescado sec./sal./en salm. har./pol./pell. aptos p/c humano', new_value='Otras preparaciones aptas para consumo humano')
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
#  replace_value(col='producto', curr_value='Harina, polvo y pellets de pescado. No aptos p/c humano', new_value='Harina, polvo y pellets de pescado. No aptos para consumo humano')
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
#  |    | gran_rubro   | producto                  |   toneladas |   fob_miles_usd |   share_toneladas |   share_fob |
#  |---:|:-------------|:--------------------------|------------:|----------------:|------------------:|------------:|
#  |  0 | MOA          | Filetes y demás carnes de |     79321.9 |          254036 |          0.152354 |     13.4415 |
#  |    |              | pescado                   |             |                 |                   |             |
#  
#  ------------------------------
#  
#  txtwrapper(col='producto', width=25)
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
#  |    | gran_rubro   | producto                  |   toneladas |   fob_miles_usd |   share_toneladas |   share_fob |
#  |---:|:-------------|:--------------------------|------------:|----------------:|------------------:|------------:|
#  |  0 | MOA          | Filetes y demás carnes de |     79321.9 |          254036 |          0.152354 |     13.4415 |
#  |    |              | pescado                   |             |                 |                   |             |
#  
#  ------------------------------
#  