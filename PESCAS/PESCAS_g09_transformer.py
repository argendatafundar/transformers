from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def txtwrapper(df: DataFrame, col:str, width:int)->DataFrame:
    import textwrap
    df[col] = df[col].apply(lambda text: "\n".join(textwrap.wrap(text, width=width)))
    return df

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	ordenar_dos_columnas(col1='pais', order1=['Bangladesh', 'Camboya', 'Sierra Leona', 'Indonesia', 'Islas Salomón', 'Maldivas', 'Sri Lanka', 'Myanmar', 'Costa de Marfil', 'Kiribati', 'Uganda', 'Ghana', 'India', 'Santo Tomé y Príncipe', 'Camerún', 'Mozambique', 'Malí', 'Tailandia', 'Nigeria', 'Islandia', 'República Democrática del Congo', 'Gambia', 'Benin', 'Guinea', 'Lao', 'Ruanda', 'Vanuatu', 'Togo', 'Micronesia', 'Vietnam', 'Japón', 'Filipinas', 'Zambia', 'Malasia', 'Tuvalu', 'Omán', 'Noruega', 'Senegal', 'Madagascar', 'Burundi', 'Seychelles', 'Corea del Sur', 'Egipto', 'Macao', 'China', 'Antigua y Barbuda', 'Comoras', 'Fiji', 'Angola', 'Tanzanía', 'Portugal', 'Marruecos', 'Hong Kong', 'Túnez', 'Congo', 'Barbados', 'Perú', 'Mauricio', 'Polinesia Francesa', 'Malta', 'Suecia', 'Finlandia', 'Libia', 'Dinamarca', 'Gabón', 'Burkina Faso', 'Jamaica', 'Moldavia', 'Samoa', 'Italia', 'Bhután', 'Francia', 'Namibia', 'Saint Kitts y Nevis', 'Granada', 'España', 'Bélgica', 'Islas Marshall', 'Santa Lucía', 'Lituania', 'Irán', 'Luxemburgo', 'Mundo', 'Taiwán', 'Ucrania', 'Belice', 'Guyana', 'Emiratos Árabes Unidos', 'Países Bajos', 'Letonia', 'Dominica', 'Malawi', 'Trinidad y Tobago', 'Costa Rica', 'Suriname', 'Nueva Zelanda', 'Rusia', 'Venezuela', 'Nueva Caledonia', 'Qatar', 'Kenia', 'Georgia', 'Djibouti', 'Chipre', 'Suiza', 'Bahamas', 'Haití', 'Grecia', 'Níger', 'Cabo Verde', 'Timor-Leste', 'Liberia', 'Bahrein', 'Mauritania', 'Nauru', 'Israel', 'Canadá', 'Irlanda', 'Australia', 'Croacia', 'Eslovenia', 'Reino Unido', 'Arabia Saudita', 'Nepal', 'Nicaragua', 'Panamá', 'Estonia', 'Tonga', 'Austria', 'Uruguay', 'Alemania', 'Estados Unidos', 'Kuwait', 'México', 'Guinea-Bissau', 'Lesotho', 'Paraguay', 'El Salvador', 'Macedonia del Norte', 'Eslovaquia', 'Albania', 'San Vicente y las Granadinas', 'Argelia', 'Chile', 'Yemen', 'Somalia', 'Eswatini', 'República Dominicana', 'Sudán del Sur', 'Colombia', 'Montenegro', 'Polonia', 'Chequia', 'Jordania', 'Belarús', 'Papua Nueva Guinea', 'Líbano', 'Iraq', 'Ecuador', 'Bulgaria', 'Chad', 'Bosnia y Herzegovina', 'Rumania', 'Turquía', 'Honduras', 'Serbia', 'Siria', 'Cuba', 'Sudáfrica', 'Uzbekistán', 'Armenia', 'Brasil', 'Hungría', 'Etiopía', 'Guatemala', 'Botswana', 'Pakistán', 'Argentina', 'Afganistán', 'Azerbaiyán', 'Kazajstán', 'Sudán', 'Zimbabwe', 'Turkmenistán', 'Kirguistán', 'Bolivia', 'Tayikistán', 'Mongolia', 'República Centroafricana'], col2='tipo_carne', order2=['Pescados y mariscos', 'Aviar', 'Vacuna', 'Porcina', 'Caprina y ovina', 'Otras carnes']),
	txtwrapper(col='tipo_carne', width=10)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1134 entries, 0 to 1133
#  Data columns (total 6 columns):
#   #   Column      Non-Null Count  Dtype   
#  ---  ------      --------------  -----   
#   0   anio        1134 non-null   int64   
#   1   iso3        1134 non-null   object  
#   2   tipo_carne  1134 non-null   category
#   3   pais        1134 non-null   category
#   4   valor       1134 non-null   float64 
#   5   share       1134 non-null   float64 
#  
#  |    |   anio | iso3   | tipo_carne   | pais       |   valor |   share |
#  |---:|-------:|:-------|:-------------|:-----------|--------:|--------:|
#  |  0 |   2022 | AFG    | Aviar        | Afganistán |    0.87 | 12.1678 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='pais', order1=['Bangladesh', 'Camboya', 'Sierra Leona', 'Indonesia', 'Islas Salomón', 'Maldivas', 'Sri Lanka', 'Myanmar', 'Costa de Marfil', 'Kiribati', 'Uganda', 'Ghana', 'India', 'Santo Tomé y Príncipe', 'Camerún', 'Mozambique', 'Malí', 'Tailandia', 'Nigeria', 'Islandia', 'República Democrática del Congo', 'Gambia', 'Benin', 'Guinea', 'Lao', 'Ruanda', 'Vanuatu', 'Togo', 'Micronesia', 'Vietnam', 'Japón', 'Filipinas', 'Zambia', 'Malasia', 'Tuvalu', 'Omán', 'Noruega', 'Senegal', 'Madagascar', 'Burundi', 'Seychelles', 'Corea del Sur', 'Egipto', 'Macao', 'China', 'Antigua y Barbuda', 'Comoras', 'Fiji', 'Angola', 'Tanzanía', 'Portugal', 'Marruecos', 'Hong Kong', 'Túnez', 'Congo', 'Barbados', 'Perú', 'Mauricio', 'Polinesia Francesa', 'Malta', 'Suecia', 'Finlandia', 'Libia', 'Dinamarca', 'Gabón', 'Burkina Faso', 'Jamaica', 'Moldavia', 'Samoa', 'Italia', 'Bhután', 'Francia', 'Namibia', 'Saint Kitts y Nevis', 'Granada', 'España', 'Bélgica', 'Islas Marshall', 'Santa Lucía', 'Lituania', 'Irán', 'Luxemburgo', 'Mundo', 'Taiwán', 'Ucrania', 'Belice', 'Guyana', 'Emiratos Árabes Unidos', 'Países Bajos', 'Letonia', 'Dominica', 'Malawi', 'Trinidad y Tobago', 'Costa Rica', 'Suriname', 'Nueva Zelanda', 'Rusia', 'Venezuela', 'Nueva Caledonia', 'Qatar', 'Kenia', 'Georgia', 'Djibouti', 'Chipre', 'Suiza', 'Bahamas', 'Haití', 'Grecia', 'Níger', 'Cabo Verde', 'Timor-Leste', 'Liberia', 'Bahrein', 'Mauritania', 'Nauru', 'Israel', 'Canadá', 'Irlanda', 'Australia', 'Croacia', 'Eslovenia', 'Reino Unido', 'Arabia Saudita', 'Nepal', 'Nicaragua', 'Panamá', 'Estonia', 'Tonga', 'Austria', 'Uruguay', 'Alemania', 'Estados Unidos', 'Kuwait', 'México', 'Guinea-Bissau', 'Lesotho', 'Paraguay', 'El Salvador', 'Macedonia del Norte', 'Eslovaquia', 'Albania', 'San Vicente y las Granadinas', 'Argelia', 'Chile', 'Yemen', 'Somalia', 'Eswatini', 'República Dominicana', 'Sudán del Sur', 'Colombia', 'Montenegro', 'Polonia', 'Chequia', 'Jordania', 'Belarús', 'Papua Nueva Guinea', 'Líbano', 'Iraq', 'Ecuador', 'Bulgaria', 'Chad', 'Bosnia y Herzegovina', 'Rumania', 'Turquía', 'Honduras', 'Serbia', 'Siria', 'Cuba', 'Sudáfrica', 'Uzbekistán', 'Armenia', 'Brasil', 'Hungría', 'Etiopía', 'Guatemala', 'Botswana', 'Pakistán', 'Argentina', 'Afganistán', 'Azerbaiyán', 'Kazajstán', 'Sudán', 'Zimbabwe', 'Turkmenistán', 'Kirguistán', 'Bolivia', 'Tayikistán', 'Mongolia', 'República Centroafricana'], col2='tipo_carne', order2=['Pescados y mariscos', 'Aviar', 'Vacuna', 'Porcina', 'Caprina y ovina', 'Otras carnes'])
#  Index: 1134 entries, 87 to 158
#  Data columns (total 6 columns):
#   #   Column      Non-Null Count  Dtype   
#  ---  ------      --------------  -----   
#   0   anio        1134 non-null   int64   
#   1   iso3        1134 non-null   object  
#   2   tipo_carne  1134 non-null   category
#   3   pais        1134 non-null   category
#   4   valor       1134 non-null   float64 
#   5   share       1134 non-null   float64 
#  
#  |    |   anio | iso3   | tipo_carne   | pais       |   valor |   share |
#  |---:|-------:|:-------|:-------------|:-----------|--------:|--------:|
#  | 87 |   2022 | BGD    | Pescados y   | Bangladesh |    25.7 | 85.5241 |
#  |    |        |        | mariscos     |            |         |         |
#  
#  ------------------------------
#  
#  txtwrapper(col='tipo_carne', width=10)
#  Index: 1134 entries, 87 to 158
#  Data columns (total 6 columns):
#   #   Column      Non-Null Count  Dtype   
#  ---  ------      --------------  -----   
#   0   anio        1134 non-null   int64   
#   1   iso3        1134 non-null   object  
#   2   tipo_carne  1134 non-null   category
#   3   pais        1134 non-null   category
#   4   valor       1134 non-null   float64 
#   5   share       1134 non-null   float64 
#  
#  |    |   anio | iso3   | tipo_carne   | pais       |   valor |   share |
#  |---:|-------:|:-------|:-------------|:-----------|--------:|--------:|
#  | 87 |   2022 | BGD    | Pescados y   | Bangladesh |    25.7 | 85.5241 |
#  |    |        |        | mariscos     |            |         |         |
#  
#  ------------------------------
#  