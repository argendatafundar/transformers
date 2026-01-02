from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	replace_multiple_values(col='clasificacion_lall', replacements={'Manufacturas en basadas en RRNN': 'Basadas en RRNN', 'Productos primarios': 'P. Primarios', 'Manufacturas de media tecnología': 'Media tecnología', 'Manufacturas de alta tecnología': 'Alta tecnología', 'Manufacturas de baja tecnología': 'Baja tecnología'}),
	drop_na(col='geonombreFundar'),
	ordenar_dos_columnas(col1='geonombreFundar', order1=['Kirguistán', 'Andorra', 'Sri Lanka', 'Gabón', 'Alemania', 'Ecuador', 'Benin', 'Francia', 'México', 'Guatemala', 'Estados Unidos', 'Myanmar', 'Georgia', 'Rep. Dominicana', 'Belice', 'El Salvador', 'Líbano', 'Uganda', 'Hungría', 'Costa de Marfil', 'Costa Rica', 'Tailandia', 'Paraguay', 'Bolivia', 'Panamá', 'Antigua y Barbuda', 'Burkina Faso', 'Uzbekistán', 'Jamaica', 'Venezuela', 'San Vicente y las Granadinas', 'Nicaragua', 'Azerbaiyán', 'Colombia', 'Ucrania', 'Uruguay', 'Sudáfrica', 'Honduras', 'Botswana', 'Suecia', 'España', 'Austria', 'Unión Europea', 'Granada', 'Moldavia', 'Túnez', 'Mundo', 'Barbados', 'Dinamarca', 'Finlandia', 'Qatar', 'Emiratos Árabes Unidos', 'Perú', 'Suriname', 'Bahrein', 'Cuba', 'Namibia', 'Pakistán', 'Australia', 'Kuwait', 'Chile', 'Kenia', 'Brasil', 'Corea del Sur', 'Canadá', 'Rumania', 'Filipinas', 'Bosnia y Herzegovina', 'Nueva Zelanda', 'Iraq', 'China', 'Japón', 'Turquía', 'Malta', 'Singapur', 'Islandia', 'Guyana', 'Seychelles', 'Hong Kong', 'Jordania', 'Serbia', 'Israel', 'Italia', 'Reino Unido', 'Vietnam', 'Indonesia', 'Armenia', 'Kazajstán', 'Tanzanía', 'Omán', 'Taiwán', 'Países Bajos', 'Egipto', 'Noruega', 'Etiopía', 'Luxemburgo', 'Macao', 'Bulgaria', 'Bélgica', 'Dominica', 'Nigeria', 'Mauricio', 'Chequia', 'Rusia', 'Polinesia Francesa', 'India', 'Arabia Saudita', 'Suiza', 'Polonia', 'Maldivas', 'Yemen', 'Mozambique', 'Letonia', 'Grecia', 'Estonia', 'Níger', 'Eslovaquia', 'Portugal', 'Lituania', 'Djibouti', 'Eslovenia', 'Marruecos', 'Angola', 'Irlanda', 'Zambia', 'Camboya', 'Montenegro', 'Malasia', 'Ghana', 'Eswatini', 'Macedonia del Norte', 'Argelia', 'Senegal', 'Croacia', 'Brunei', 'Madagascar', 'Chipre'], col2='clasificacion_lall', order2=['Alta tecnología', 'Media tecnología', 'Baja tecnología', 'Basadas en RRNN', 'P. Primarios'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 17875 entries, 0 to 17874
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                17875 non-null  int64  
#   1   importer_iso3       17875 non-null  object 
#   2   geonombreFundar     17875 non-null  object 
#   3   clasificacion_lall  17875 non-null  object 
#   4   expo                17875 non-null  float64
#   5   prop                17875 non-null  float64
#  
#  |    |   anio | importer_iso3   | geonombreFundar   | clasificacion_lall              |   expo |   prop |
#  |---:|-------:|:----------------|:------------------|:--------------------------------|-------:|-------:|
#  |  0 |   2002 | AFG             | Afganistán        | Manufacturas en basadas en RRNN |  84.33 |    100 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 721 entries, 3813 to 17874
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                721 non-null    int64  
#   1   importer_iso3       721 non-null    object 
#   2   geonombreFundar     721 non-null    object 
#   3   clasificacion_lall  721 non-null    object 
#   4   expo                721 non-null    float64
#   5   prop                721 non-null    float64
#  
#  |      |   anio | importer_iso3   | geonombreFundar   | clasificacion_lall   |    expo |    prop |
#  |-----:|-------:|:----------------|:------------------|:---------------------|--------:|--------:|
#  | 3813 |   2023 | ALB             | Albania           | Productos primarios  | 7655.34 | 82.1898 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='clasificacion_lall', replacements={'Manufacturas en basadas en RRNN': 'Basadas en RRNN', 'Productos primarios': 'P. Primarios', 'Manufacturas de media tecnología': 'Media tecnología', 'Manufacturas de alta tecnología': 'Alta tecnología', 'Manufacturas de baja tecnología': 'Baja tecnología'})
#  Index: 721 entries, 3813 to 17874
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                721 non-null    int64  
#   1   importer_iso3       721 non-null    object 
#   2   geonombreFundar     721 non-null    object 
#   3   clasificacion_lall  721 non-null    object 
#   4   expo                721 non-null    float64
#   5   prop                721 non-null    float64
#  
#  |      |   anio | importer_iso3   | geonombreFundar   | clasificacion_lall   |    expo |    prop |
#  |-----:|-------:|:----------------|:------------------|:---------------------|--------:|--------:|
#  | 3813 |   2023 | ALB             | Albania           | P. Primarios         | 7655.34 | 82.1898 |
#  
#  ------------------------------
#  
#  drop_na(col='geonombreFundar')
#  Index: 721 entries, 3813 to 17874
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype   
#  ---  ------              --------------  -----   
#   0   anio                721 non-null    int64   
#   1   importer_iso3       721 non-null    object  
#   2   geonombreFundar     662 non-null    category
#   3   clasificacion_lall  721 non-null    category
#   4   expo                721 non-null    float64 
#   5   prop                721 non-null    float64 
#  
#  |      |   anio | importer_iso3   |   geonombreFundar | clasificacion_lall   |    expo |    prop |
#  |-----:|-------:|:----------------|------------------:|:---------------------|--------:|--------:|
#  | 3813 |   2023 | ALB             |               nan | P. Primarios         | 7655.34 | 82.1898 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geonombreFundar', order1=['Kirguistán', 'Andorra', 'Sri Lanka', 'Gabón', 'Alemania', 'Ecuador', 'Benin', 'Francia', 'México', 'Guatemala', 'Estados Unidos', 'Myanmar', 'Georgia', 'Rep. Dominicana', 'Belice', 'El Salvador', 'Líbano', 'Uganda', 'Hungría', 'Costa de Marfil', 'Costa Rica', 'Tailandia', 'Paraguay', 'Bolivia', 'Panamá', 'Antigua y Barbuda', 'Burkina Faso', 'Uzbekistán', 'Jamaica', 'Venezuela', 'San Vicente y las Granadinas', 'Nicaragua', 'Azerbaiyán', 'Colombia', 'Ucrania', 'Uruguay', 'Sudáfrica', 'Honduras', 'Botswana', 'Suecia', 'España', 'Austria', 'Unión Europea', 'Granada', 'Moldavia', 'Túnez', 'Mundo', 'Barbados', 'Dinamarca', 'Finlandia', 'Qatar', 'Emiratos Árabes Unidos', 'Perú', 'Suriname', 'Bahrein', 'Cuba', 'Namibia', 'Pakistán', 'Australia', 'Kuwait', 'Chile', 'Kenia', 'Brasil', 'Corea del Sur', 'Canadá', 'Rumania', 'Filipinas', 'Bosnia y Herzegovina', 'Nueva Zelanda', 'Iraq', 'China', 'Japón', 'Turquía', 'Malta', 'Singapur', 'Islandia', 'Guyana', 'Seychelles', 'Hong Kong', 'Jordania', 'Serbia', 'Israel', 'Italia', 'Reino Unido', 'Vietnam', 'Indonesia', 'Armenia', 'Kazajstán', 'Tanzanía', 'Omán', 'Taiwán', 'Países Bajos', 'Egipto', 'Noruega', 'Etiopía', 'Luxemburgo', 'Macao', 'Bulgaria', 'Bélgica', 'Dominica', 'Nigeria', 'Mauricio', 'Chequia', 'Rusia', 'Polinesia Francesa', 'India', 'Arabia Saudita', 'Suiza', 'Polonia', 'Maldivas', 'Yemen', 'Mozambique', 'Letonia', 'Grecia', 'Estonia', 'Níger', 'Eslovaquia', 'Portugal', 'Lituania', 'Djibouti', 'Eslovenia', 'Marruecos', 'Angola', 'Irlanda', 'Zambia', 'Camboya', 'Montenegro', 'Malasia', 'Ghana', 'Eswatini', 'Macedonia del Norte', 'Argelia', 'Senegal', 'Croacia', 'Brunei', 'Madagascar', 'Chipre'], col2='clasificacion_lall', order2=['Alta tecnología', 'Media tecnología', 'Baja tecnología', 'Basadas en RRNN', 'P. Primarios'])
#  Index: 721 entries, 8362 to 17260
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype   
#  ---  ------              --------------  -----   
#   0   anio                721 non-null    int64   
#   1   importer_iso3       721 non-null    object  
#   2   geonombreFundar     662 non-null    category
#   3   clasificacion_lall  721 non-null    category
#   4   expo                721 non-null    float64 
#   5   prop                721 non-null    float64 
#  
#  |      |   anio | importer_iso3   | geonombreFundar   | clasificacion_lall   |   expo |   prop |
#  |-----:|-------:|:----------------|:------------------|:---------------------|-------:|-------:|
#  | 8362 |   2023 | KGZ             | Kirguistán        | Alta tecnología      | 769.49 | 66.181 |
#  
#  ------------------------------
#  