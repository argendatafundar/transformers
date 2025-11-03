from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='valor', k=100),
	query(condition="apertura_sexo.isin(['mujer','varon'])"),
	query(condition='anio == anio.max()'),
	replace_multiple_values(col='apertura_sexo', replacements={'varon': 'Varones', 'mujer': 'Mujeres'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2592 entries, 0 to 2591
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           2592 non-null   int64  
#   1   edad           2592 non-null   int64  
#   2   apertura_sexo  2592 non-null   object 
#   3   valor          2592 non-null   float64
#  
#  |    |   anio |   edad | apertura_sexo   |      valor |
#  |---:|-------:|-------:|:----------------|-----------:|
#  |  0 |   2016 |     10 | total           | 0.00156134 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 2592 entries, 0 to 2591
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           2592 non-null   int64  
#   1   edad           2592 non-null   int64  
#   2   apertura_sexo  2592 non-null   object 
#   3   valor          2592 non-null   float64
#  
#  |    |   anio |   edad | apertura_sexo   |    valor |
#  |---:|-------:|-------:|:----------------|---------:|
#  |  0 |   2016 |     10 | total           | 0.156134 |
#  
#  ------------------------------
#  
#  query(condition="apertura_sexo.isin(['mujer','varon'])")
#  Index: 1296 entries, 1 to 2590
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           1296 non-null   int64  
#   1   edad           1296 non-null   int64  
#   2   apertura_sexo  1296 non-null   object 
#   3   valor          1296 non-null   float64
#  
#  |    |   anio |   edad | apertura_sexo   |    valor |
#  |---:|-------:|-------:|:----------------|---------:|
#  |  1 |   2016 |     10 | varon           | 0.304733 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 162 entries, 2269 to 2590
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           162 non-null    int64  
#   1   edad           162 non-null    int64  
#   2   apertura_sexo  162 non-null    object 
#   3   valor          162 non-null    float64
#  
#  |      |   anio |   edad | apertura_sexo   |      valor |
#  |-----:|-------:|-------:|:----------------|-----------:|
#  | 2269 |   2023 |     10 | varon           | 0.00777848 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='apertura_sexo', replacements={'varon': 'Varones', 'mujer': 'Mujeres'})
#  Index: 162 entries, 2269 to 2590
#  Data columns (total 4 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           162 non-null    int64  
#   1   edad           162 non-null    int64  
#   2   apertura_sexo  162 non-null    object 
#   3   valor          162 non-null    float64
#  
#  |      |   anio |   edad | apertura_sexo   |      valor |
#  |-----:|-------:|-------:|:----------------|-----------:|
#  | 2269 |   2023 |     10 | Varones         | 0.00777848 |
#  
#  ------------------------------
#  