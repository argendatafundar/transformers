from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_col(col='geocodigoFundar', axis=1),
	query(condition='componente_egresos_del_gobierno != "TOTAL"'),
	ordenar_dos_columnas(col1='geonombreFundar', order1=['Francia', 'Brasil', 'Grecia', 'Bélgica', 'Italia', 'Dinamarca', 'Finlandia', 'Austria', 'Letonia', 'Suecia', 'Portugal', 'Noruega', 'Eslovenia', 'Hungría', 'España', 'Alemania', 'Países Bajos', 'Chequia', 'Polonia', 'Argentina', 'Reino Unido', 'Japón', 'Eslovaquia', 'Estonia', 'Canadá', 'Israel', 'Estados Unidos', 'Nueva Zelanda', 'Luxemburgo', 'Rusia', 'Lituania', 'Irlanda', 'Sudáfrica', 'Colombia', 'Turquía', 'Costa Rica', 'Suiza', 'Corea del Sur', 'México', 'China', 'Chile'], col2='componente_egresos_del_gobierno', order2=['Consumo del gobierno', 'Subvenciones económicas', 'Rentas / Intereses', 'Prestaciones sociales', 'Otras transferencias corrientes y de capital', 'Formación bruta de capital'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 287 entries, 0 to 286
#  Data columns (total 4 columns):
#   #   Column                           Non-Null Count  Dtype 
#  ---  ------                           --------------  ----- 
#   0   geocodigoFundar                  287 non-null    object
#   1   geonombreFundar                  287 non-null    object
#   2   componente_egresos_del_gobierno  287 non-null    object
#   3   porcentaje_del_pib               287 non-null    int64 
#  
#  |    | geocodigoFundar   | geonombreFundar   | componente_egresos_del_gobierno   |   porcentaje_del_pib |
#  |---:|:------------------|:------------------|:----------------------------------|---------------------:|
#  |  0 | ARG               | Argentina         | Consumo del gobierno              |                   17 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  RangeIndex: 287 entries, 0 to 286
#  Data columns (total 3 columns):
#   #   Column                           Non-Null Count  Dtype 
#  ---  ------                           --------------  ----- 
#   0   geonombreFundar                  287 non-null    object
#   1   componente_egresos_del_gobierno  287 non-null    object
#   2   porcentaje_del_pib               287 non-null    int64 
#  
#  |    | geonombreFundar   | componente_egresos_del_gobierno   |   porcentaje_del_pib |
#  |---:|:------------------|:----------------------------------|---------------------:|
#  |  0 | Argentina         | Consumo del gobierno              |                   17 |
#  
#  ------------------------------
#  
#  query(condition='componente_egresos_del_gobierno != "TOTAL"')
#  Index: 246 entries, 0 to 285
#  Data columns (total 3 columns):
#   #   Column                           Non-Null Count  Dtype   
#  ---  ------                           --------------  -----   
#   0   geonombreFundar                  246 non-null    category
#   1   componente_egresos_del_gobierno  246 non-null    category
#   2   porcentaje_del_pib               246 non-null    int64   
#  
#  |    | geonombreFundar   | componente_egresos_del_gobierno   |   porcentaje_del_pib |
#  |---:|:------------------|:----------------------------------|---------------------:|
#  |  0 | Argentina         | Consumo del gobierno              |                   17 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geonombreFundar', order1=['Francia', 'Brasil', 'Grecia', 'Bélgica', 'Italia', 'Dinamarca', 'Finlandia', 'Austria', 'Letonia', 'Suecia', 'Portugal', 'Noruega', 'Eslovenia', 'Hungría', 'España', 'Alemania', 'Países Bajos', 'Chequia', 'Polonia', 'Argentina', 'Reino Unido', 'Japón', 'Eslovaquia', 'Estonia', 'Canadá', 'Israel', 'Estados Unidos', 'Nueva Zelanda', 'Luxemburgo', 'Rusia', 'Lituania', 'Irlanda', 'Sudáfrica', 'Colombia', 'Turquía', 'Costa Rica', 'Suiza', 'Corea del Sur', 'México', 'China', 'Chile'], col2='componente_egresos_del_gobierno', order2=['Consumo del gobierno', 'Subvenciones económicas', 'Rentas / Intereses', 'Prestaciones sociales', 'Otras transferencias corrientes y de capital', 'Formación bruta de capital'])
#  Index: 246 entries, 77 to 12
#  Data columns (total 3 columns):
#   #   Column                           Non-Null Count  Dtype   
#  ---  ------                           --------------  -----   
#   0   geonombreFundar                  246 non-null    category
#   1   componente_egresos_del_gobierno  246 non-null    category
#   2   porcentaje_del_pib               246 non-null    int64   
#  
#  |    | geonombreFundar   | componente_egresos_del_gobierno   |   porcentaje_del_pib |
#  |---:|:------------------|:----------------------------------|---------------------:|
#  | 77 | Francia           | Consumo del gobierno              |                   24 |
#  
#  ------------------------------
#  