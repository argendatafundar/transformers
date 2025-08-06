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
	query(condition='anio in [1900, 1925, 1950, 1975, 2000, 2022]'),
	drop_col(col=['geocodigoFundar'], axis=1),
	ordenar_dos_columnas(col1='geonombreFundar', order1=['Afganistán', 'Albania', 'Alemania', 'América Latina', 'Angola', 'Arabia Saudita', 'Argelia', 'Argentina', 'Armenia', 'Asia Oriental', 'Asia del Sur y Sudeste', 'Australia', 'Austria', 'Azerbaiyán', 'Bahrein', 'Bangladesh', 'Barbados', 'Belarús', 'Benin', 'Bolivia', 'Bosnia y Herzegovina', 'Botswana', 'Brasil', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Bélgica', 'Cabo Verde', 'Camboya', 'Camerún', 'Canadá', 'Chad', 'Checoslovaquia', 'Chequia', 'Chile', 'China', 'Chipre', 'Colombia', 'Comoras', 'Congo', 'Corea del Norte', 'Corea del Sur', 'Costa Rica', 'Costa de Marfil', 'Croacia', 'Cuba', 'Dinamarca', 'Djibouti', 'Dominica', 'Ecuador', 'Egipto', 'El Salvador', 'Emiratos Árabes Unidos', 'Eslovaquia', 'Eslovenia', 'España', 'Estados Unidos', 'Estonia', 'Eswatini', 'Etiopía', 'Europa Occidental', 'Europa Oriental', 'Filipinas', 'Finlandia', 'Francia', 'Gabón', 'Gambia', 'Georgia', 'Ghana', 'Grecia', 'Guatemala', 'Guinea', 'Guinea Ecuatorial', 'Guinea-Bissau', 'Haití', 'Honduras', 'Hong Kong', 'Hungría', 'India', 'Indonesia', 'Iraq', 'Irlanda', 'Irán', 'Islandia', 'Israel', 'Italia', 'Jamaica', 'Japón', 'Jordania', 'Kazajstán', 'Kenia', 'Kirguistán', 'Kuwait', 'Laos', 'Lesotho', 'Letonia', 'Liberia', 'Libia', 'Lituania', 'Luxemburgo', 'Líbano', 'Macedonia del Norte', 'Madagascar', 'Malasia', 'Malawi', 'Malta', 'Malí', 'Marruecos', 'Mauricio', 'Mauritania', 'Medio Oriente y África del Norte', 'Moldavia', 'Mongolia', 'Montenegro', 'Mozambique', 'Mundo', 'Myanmar', 'México', 'Namibia', 'Nepal', 'Nicaragua', 'Nigeria', 'Noruega', 'Nueva Zelanda', 'Níger', 'Omán', 'Pakistán', 'Palestina', 'Panamá', 'Paraguay', 'Países Bajos', 'Perú', 'Polonia', 'Portugal', 'Puerto Rico', 'Qatar', 'Ramificaciones de Occidente', 'Reino Unido', 'Rep. Centroafricana', 'Rep. Dem. Congo', 'Rep. Dominicana', 'Ruanda', 'Rumania', 'Rusia', 'Santa Lucía', 'Santo Tomé y Príncipe', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leona', 'Singapur', 'Siria', 'Sri Lanka', 'Sudáfrica', 'Sudán', 'Suecia', 'Suiza', 'Tailandia', 'Taiwán', 'Tanzanía', 'Tayikistán', 'Togo', 'Trinidad y Tobago', 'Turkmenistán', 'Turquía', 'Túnez', 'Ucrania', 'Uganda', 'Unión Soviética', 'Uruguay', 'Uzbekistán', 'Venezuela', 'Vietnam', 'Yemen', 'Yugoslavia', 'Zambia', 'Zimbabwe', 'África Subsahariana'], col2='anio', order2=[1900, 1925, 1950, 1975, 2000, 2022])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21586 entries, 0 to 21585
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  21586 non-null  object 
#   1   geonombreFundar  21586 non-null  object 
#   2   anio             21586 non-null  int64  
#   3   pib_per_capita   21586 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |
#  |---:|:------------------|:------------------|-------:|-----------------:|
#  |  0 | AFG               | Afganistán        |   1950 |             1156 |
#  
#  ------------------------------
#  
#  query(condition='anio in [1900, 1925, 1950, 1975, 2000, 2022]')
#  Index: 785 entries, 0 to 21585
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  785 non-null    object 
#   1   geonombreFundar  785 non-null    object 
#   2   anio             785 non-null    int64  
#   3   pib_per_capita   785 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   pib_per_capita |
#  |---:|:------------------|:------------------|-------:|-----------------:|
#  |  0 | AFG               | Afganistán        |   1950 |             1156 |
#  
#  ------------------------------
#  
#  drop_col(col=['geocodigoFundar'], axis=1)
#  Index: 785 entries, 0 to 21585
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  785 non-null    category
#   1   anio             785 non-null    category
#   2   pib_per_capita   785 non-null    float64 
#  
#  |    | geonombreFundar   |   anio |   pib_per_capita |
#  |---:|:------------------|-------:|-----------------:|
#  |  0 | Afganistán        |   1950 |             1156 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geonombreFundar', order1=['Afganistán', 'Albania', 'Alemania', 'América Latina', 'Angola', 'Arabia Saudita', 'Argelia', 'Argentina', 'Armenia', 'Asia Oriental', 'Asia del Sur y Sudeste', 'Australia', 'Austria', 'Azerbaiyán', 'Bahrein', 'Bangladesh', 'Barbados', 'Belarús', 'Benin', 'Bolivia', 'Bosnia y Herzegovina', 'Botswana', 'Brasil', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Bélgica', 'Cabo Verde', 'Camboya', 'Camerún', 'Canadá', 'Chad', 'Checoslovaquia', 'Chequia', 'Chile', 'China', 'Chipre', 'Colombia', 'Comoras', 'Congo', 'Corea del Norte', 'Corea del Sur', 'Costa Rica', 'Costa de Marfil', 'Croacia', 'Cuba', 'Dinamarca', 'Djibouti', 'Dominica', 'Ecuador', 'Egipto', 'El Salvador', 'Emiratos Árabes Unidos', 'Eslovaquia', 'Eslovenia', 'España', 'Estados Unidos', 'Estonia', 'Eswatini', 'Etiopía', 'Europa Occidental', 'Europa Oriental', 'Filipinas', 'Finlandia', 'Francia', 'Gabón', 'Gambia', 'Georgia', 'Ghana', 'Grecia', 'Guatemala', 'Guinea', 'Guinea Ecuatorial', 'Guinea-Bissau', 'Haití', 'Honduras', 'Hong Kong', 'Hungría', 'India', 'Indonesia', 'Iraq', 'Irlanda', 'Irán', 'Islandia', 'Israel', 'Italia', 'Jamaica', 'Japón', 'Jordania', 'Kazajstán', 'Kenia', 'Kirguistán', 'Kuwait', 'Laos', 'Lesotho', 'Letonia', 'Liberia', 'Libia', 'Lituania', 'Luxemburgo', 'Líbano', 'Macedonia del Norte', 'Madagascar', 'Malasia', 'Malawi', 'Malta', 'Malí', 'Marruecos', 'Mauricio', 'Mauritania', 'Medio Oriente y África del Norte', 'Moldavia', 'Mongolia', 'Montenegro', 'Mozambique', 'Mundo', 'Myanmar', 'México', 'Namibia', 'Nepal', 'Nicaragua', 'Nigeria', 'Noruega', 'Nueva Zelanda', 'Níger', 'Omán', 'Pakistán', 'Palestina', 'Panamá', 'Paraguay', 'Países Bajos', 'Perú', 'Polonia', 'Portugal', 'Puerto Rico', 'Qatar', 'Ramificaciones de Occidente', 'Reino Unido', 'Rep. Centroafricana', 'Rep. Dem. Congo', 'Rep. Dominicana', 'Ruanda', 'Rumania', 'Rusia', 'Santa Lucía', 'Santo Tomé y Príncipe', 'Senegal', 'Serbia', 'Seychelles', 'Sierra Leona', 'Singapur', 'Siria', 'Sri Lanka', 'Sudáfrica', 'Sudán', 'Suecia', 'Suiza', 'Tailandia', 'Taiwán', 'Tanzanía', 'Tayikistán', 'Togo', 'Trinidad y Tobago', 'Turkmenistán', 'Turquía', 'Túnez', 'Ucrania', 'Uganda', 'Unión Soviética', 'Uruguay', 'Uzbekistán', 'Venezuela', 'Vietnam', 'Yemen', 'Yugoslavia', 'Zambia', 'Zimbabwe', 'África Subsahariana'], col2='anio', order2=[1900, 1925, 1950, 1975, 2000, 2022])
#  Index: 785 entries, 0 to 21582
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  785 non-null    category
#   1   anio             785 non-null    category
#   2   pib_per_capita   785 non-null    float64 
#  
#  |    | geonombreFundar   |   anio |   pib_per_capita |
#  |---:|:------------------|-------:|-----------------:|
#  |  0 | Afganistán        |   1950 |             1156 |
#  
#  ------------------------------
#  