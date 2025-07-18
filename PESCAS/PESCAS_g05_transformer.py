from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='share_fob', k=100),
	replace_value(col='especie', curr_value='Merluza Hubbsi', new_value='Merluza hubbsi'),
	replace_value(col='especie', curr_value='Calamar Illex', new_value='Calamar illex')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          4 non-null      int64  
#   1   especie       4 non-null      object 
#   2   fob_mill_usd  4 non-null      float64
#   3   share_fob     4 non-null      float64
#  
#  |    |   anio | especie       |   fob_mill_usd |   share_fob |
#  |---:|-------:|:--------------|---------------:|------------:|
#  |  0 |   2024 | Calamar Illex |        372.706 |    0.208227 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='share_fob', k=100)
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          4 non-null      int64  
#   1   especie       4 non-null      object 
#   2   fob_mill_usd  4 non-null      float64
#   3   share_fob     4 non-null      float64
#  
#  |    |   anio | especie       |   fob_mill_usd |   share_fob |
#  |---:|-------:|:--------------|---------------:|------------:|
#  |  0 |   2024 | Calamar Illex |        372.706 |     20.8227 |
#  
#  ------------------------------
#  
#  replace_value(col='especie', curr_value='Merluza Hubbsi', new_value='Merluza hubbsi')
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          4 non-null      int64  
#   1   especie       4 non-null      object 
#   2   fob_mill_usd  4 non-null      float64
#   3   share_fob     4 non-null      float64
#  
#  |    |   anio | especie       |   fob_mill_usd |   share_fob |
#  |---:|-------:|:--------------|---------------:|------------:|
#  |  0 |   2024 | Calamar Illex |        372.706 |     20.8227 |
#  
#  ------------------------------
#  
#  replace_value(col='especie', curr_value='Calamar Illex', new_value='Calamar illex')
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          4 non-null      int64  
#   1   especie       4 non-null      object 
#   2   fob_mill_usd  4 non-null      float64
#   3   share_fob     4 non-null      float64
#  
#  |    |   anio | especie       |   fob_mill_usd |   share_fob |
#  |---:|-------:|:--------------|---------------:|------------:|
#  |  0 |   2024 | Calamar illex |        372.706 |     20.8227 |
#  
#  ------------------------------
#  