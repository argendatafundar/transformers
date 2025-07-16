from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    data = df.copy()
    data[col] = data[col]*k
    return data

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='share', k=100),
	sort_values(how='descending', by=['share'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7 entries, 0 to 6
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     7 non-null      int64  
#   1   disciplina_de_formacion  7 non-null      object 
#   2   personas_fisicas         7 non-null      int64  
#   3   share                    7 non-null      float64
#  
#  |    |   anio | disciplina_de_formacion           |   personas_fisicas |     share |
#  |---:|-------:|:----------------------------------|-------------------:|----------:|
#  |  0 |   2023 | Ciencias Agrícolas y Veterinarias |               9203 | 0.0934049 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='share', k=100)
#  RangeIndex: 7 entries, 0 to 6
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     7 non-null      int64  
#   1   disciplina_de_formacion  7 non-null      object 
#   2   personas_fisicas         7 non-null      int64  
#   3   share                    7 non-null      float64
#  
#  |    |   anio | disciplina_de_formacion           |   personas_fisicas |   share |
#  |---:|-------:|:----------------------------------|-------------------:|--------:|
#  |  0 |   2023 | Ciencias Agrícolas y Veterinarias |               9203 | 9.34049 |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by=['share'])
#  RangeIndex: 7 entries, 0 to 6
#  Data columns (total 4 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     7 non-null      int64  
#   1   disciplina_de_formacion  7 non-null      object 
#   2   personas_fisicas         7 non-null      int64  
#   3   share                    7 non-null      float64
#  
#  |    |   anio | disciplina_de_formacion   |   personas_fisicas |   share |
#  |---:|-------:|:--------------------------|-------------------:|--------:|
#  |  0 |   2023 | Ciencias Sociales         |              24160 | 24.5209 |
#  
#  ------------------------------
#  