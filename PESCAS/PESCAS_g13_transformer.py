from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='especie_agregada', curr_value='Merluza Hubbsi', new_value='Merluza hubbsi'),
	replace_value(col='especie_agregada', curr_value='Calamar Illex', new_value='Calamar illex')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 144 entries, 0 to 143
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   144 non-null    int64  
#   1   especie_agregada       144 non-null    object 
#   2   desembarque_toneladas  144 non-null    float64
#  
#  |    |   anio | especie_agregada   |   desembarque_toneladas |
#  |---:|-------:|:-------------------|------------------------:|
#  |  0 |   1989 | Calamar Illex      |                 23101.7 |
#  
#  ------------------------------
#  
#  replace_value(col='especie_agregada', curr_value='Merluza Hubbsi', new_value='Merluza hubbsi')
#  RangeIndex: 144 entries, 0 to 143
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   144 non-null    int64  
#   1   especie_agregada       144 non-null    object 
#   2   desembarque_toneladas  144 non-null    float64
#  
#  |    |   anio | especie_agregada   |   desembarque_toneladas |
#  |---:|-------:|:-------------------|------------------------:|
#  |  0 |   1989 | Calamar Illex      |                 23101.7 |
#  
#  ------------------------------
#  
#  replace_value(col='especie_agregada', curr_value='Calamar Illex', new_value='Calamar illex')
#  RangeIndex: 144 entries, 0 to 143
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   anio                   144 non-null    int64  
#   1   especie_agregada       144 non-null    object 
#   2   desembarque_toneladas  144 non-null    float64
#  
#  |    |   anio | especie_agregada   |   desembarque_toneladas |
#  |---:|-------:|:-------------------|------------------------:|
#  |  0 |   1989 | Calamar illex      |                 23101.7 |
#  
#  ------------------------------
#  