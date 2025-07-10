from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def completar_combinaciones(df:DataFrame, keys:list[str]):

    import pandas as pd

    niveles = [df[key].dropna().unique() for key in keys]
    combinaciones = pd.MultiIndex.from_product(niveles, names=keys).to_frame(index=False)
    df = combinaciones.merge(df, on=keys, how='left')
    return df

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def fill_na(df:DataFrame, col:str, fill:Any):
    df[col] = df[col].fillna(fill)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col=['anio', 'geocodigoFundar'], axis=1),
	completar_combinaciones(keys=['geonombreFundar', 'disciplina']),
	fill_na(col='share', fill=0),
	ordenar_dos_columnas(col1='geonombreFundar', order1=['Guyana', 'Costa Rica', 'Uruguay', 'Honduras', 'Argentina', 'Brasil', 'Bolivia', 'Paraguay', 'Nicaragua', 'México', 'Puerto Rico', 'América Latina y el Caribe', 'Iberoamérica', 'Guatemala', 'España', 'Venezuela', 'El Salvador', 'Cuba', 'Barbados', 'Trinidad y Tobago', 'Rep. Dominicana', 'Ecuador', 'Perú', 'Haití', 'Portugal', 'Colombia', 'Chile', 'Jamaica'], col2='disciplina', order2=['Ciencias de la Vida', 'Ciencias Sociales', 'Ciencias de la Salud', 'Ciencias Físicas'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 112 entries, 0 to 111
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  112 non-null    object 
#   1   geonombreFundar  112 non-null    object 
#   2   anio             112 non-null    int64  
#   3   disciplina       112 non-null    object 
#   4   share            112 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | disciplina           |   share |
#  |---:|:------------------|:------------------|-------:|:---------------------|--------:|
#  |  0 | ARG               | Argentina         |   2022 | Ciencias de la Salud | 24.3324 |
#  
#  ------------------------------
#  
#  drop_col(col=['anio', 'geocodigoFundar'], axis=1)
#  RangeIndex: 112 entries, 0 to 111
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  112 non-null    object 
#   1   disciplina       112 non-null    object 
#   2   share            112 non-null    float64
#  
#  |    | geonombreFundar   | disciplina           |   share |
#  |---:|:------------------|:---------------------|--------:|
#  |  0 | Argentina         | Ciencias de la Salud | 24.3324 |
#  
#  ------------------------------
#  
#  completar_combinaciones(keys=['geonombreFundar', 'disciplina'])
#  RangeIndex: 112 entries, 0 to 111
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  112 non-null    category
#   1   disciplina       112 non-null    category
#   2   share            112 non-null    float64 
#  
#  |    | geonombreFundar   | disciplina           |   share |
#  |---:|:------------------|:---------------------|--------:|
#  |  0 | Argentina         | Ciencias de la Salud | 24.3324 |
#  
#  ------------------------------
#  
#  fill_na(col='share', fill=0)
#  RangeIndex: 112 entries, 0 to 111
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  112 non-null    category
#   1   disciplina       112 non-null    category
#   2   share            112 non-null    float64 
#  
#  |    | geonombreFundar   | disciplina           |   share |
#  |---:|:------------------|:---------------------|--------:|
#  |  0 | Argentina         | Ciencias de la Salud | 24.3324 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geonombreFundar', order1=['Guyana', 'Costa Rica', 'Uruguay', 'Honduras', 'Argentina', 'Brasil', 'Bolivia', 'Paraguay', 'Nicaragua', 'México', 'Puerto Rico', 'América Latina y el Caribe', 'Iberoamérica', 'Guatemala', 'España', 'Venezuela', 'El Salvador', 'Cuba', 'Barbados', 'Trinidad y Tobago', 'Rep. Dominicana', 'Ecuador', 'Perú', 'Haití', 'Portugal', 'Colombia', 'Chile', 'Jamaica'], col2='disciplina', order2=['Ciencias de la Vida', 'Ciencias Sociales', 'Ciencias de la Salud', 'Ciencias Físicas'])
#  Index: 112 entries, 49 to 62
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  112 non-null    category
#   1   disciplina       112 non-null    category
#   2   share            112 non-null    float64 
#  
#  |    | geonombreFundar   | disciplina          |   share |
#  |---:|:------------------|:--------------------|--------:|
#  | 49 | Guyana            | Ciencias de la Vida |   34.57 |
#  
#  ------------------------------
#  