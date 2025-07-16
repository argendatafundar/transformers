from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
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
	rename_cols(map={'tipo_carne': 'indicador'}),
	replace_value(col='indicador', curr_value='pescado_mariscos', new_value='Pescados y mariscos'),
	replace_value(col='indicador', curr_value='otras_carnes', new_value='Otras carnes'),
	replace_value(col='indicador', curr_value='aviar', new_value='Aviar'),
	replace_value(col='indicador', curr_value='caprina_ovina', new_value='Caprina y ovina'),
	replace_value(col='indicador', curr_value='porcina', new_value='Porcina'),
	replace_value(col='indicador', curr_value='vacuna', new_value='Vacuna'),
	query(condition="geocodigoFundar != 'F351'"),
	ordenar_dos_columnas(col1='geonombreFundar', order1=['Argentina', 'Zimbabwe', 'Estados Unidos', 'Australia', 'Brasil', 'Uzbekistán', 'Canadá', 'Chad', 'Israel', 'Mongolia', 'Kazajstán', 'Tayikistán', 'Luxemburgo', 'Armenia', 'Dinamarca', 'Paraguay', 'Chile', 'Malta', 'Hong Kong', 'Suecia', 'Turkmenistán', 'Bolivia', 'Francia', 'Polinesia Francesa', 'Irlanda', 'Uruguay', 'Suiza', 'Rep. Centroafricana', 'Portugal', 'Finlandia', 'Belarús', 'Noruega', 'Nueva Zelanda', 'Nueva Caledonia', 'Reino Unido', 'Sudáfrica', 'Nauru', 'Corea del Sur', 'Italia', 'Turquía', 'Países Bajos', 'Eswatini', 'Bahrein', 'Kirguistán', 'Austria', 'Eslovenia', 'Montenegro', 'México', 'Alemania', 'Panamá', 'Grecia', 'Bélgica', 'Colombia', 'Ecuador', 'Azerbaiyán', 'Sudán del Sur', 'Islandia', 'Rusia', 'Bosnia y Herzegovina', 'España', 'Guatemala', 'Venezuela', 'Albania', 'Costa Rica', 'Croacia', 'Líbano', 'Chequia', 'Omán', 'Japón', 'Pakistán', 'Myanmar', 'Botswana', 'Bhután', 'Kuwait', 'Zambia', 'Macao', 'Namibia', 'Qatar', 'Seychelles', 'Estonia', 'El Salvador', 'Laos', 'San Vicente y las Granadinas', 'Barbados', 'Guinea', 'Nepal', 'Dominica', 'Sudán', 'Marruecos', 'Tanzanía', 'Cuba', 'Irán', 'Serbia', 'Ucrania', 'Emiratos Árabes Unidos', 'Vanuatu', 'Egipto', 'Macedonia del Norte', 'China', 'Mauritania', 'Samoa', 'Jordania', 'Maldivas', 'Rep. Dominicana', 'Vietnam', 'Georgia', 'Taiwán', 'Honduras', 'Malasia', 'Eslovaquia', 'Chipre', 'Lituania', 'Djibouti', 'Antigua y Barbuda', 'Letonia', 'Trinidad y Tobago', 'Rumania', 'Burkina Faso', 'Senegal', 'Bahamas', 'Micronesia', 'Kenia', 'Gabón', 'Hungría', 'Mauricio', 'Haití', 'Camboya', 'Arabia Saudita', 'Suriname', 'Perú', 'Guyana', 'Jamaica', 'Argelia', 'Santa Lucía', 'Granada', 'Etiopía', 'Uganda', 'Belice', 'Guinea-Bissau', 'Túnez', 'Benin', 'Bulgaria', 'Malí', 'Angola', 'Saint Kitts y Nevis', 'Comoras', 'Afganistán', 'Gambia', 'Camerún', 'Fiji', 'Filipinas', 'Libia', 'Malawi', 'Níger', 'Indonesia', 'Yemen', 'Ruanda', 'Moldavia', 'Iraq', 'Nicaragua', 'Congo', 'Lesotho', 'Kiribati', 'Siria', 'Islas Salomón', 'Santo Tomé y Príncipe', 'Cabo Verde', 'Timor-Leste', 'Nigeria', 'Polonia', 'Ghana', 'Costa de Marfil', 'Madagascar', 'Bangladesh', 'Sri Lanka', 'Sierra Leona', 'Tailandia', 'India', 'Burundi', 'Corea del Norte', 'Togo', 'Papúa Nueva Guinea', 'Mozambique', 'Liberia', 'Rep. Dem. Congo'], col2='indicador', order2=['Vacuna', 'Porcina', 'Aviar', 'Pescados y mariscos', 'Caprina y ovina', 'Otras carnes'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1110 non-null   object 
#   1   geonombreFundar  1110 non-null   object 
#   2   tipo_carne       1110 non-null   object 
#   3   valor            1107 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | tipo_carne       |    valor |
#  |---:|:------------------|:------------------|:-----------------|---------:|
#  |  0 | AFG               | Afganistán        | pescado_mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_carne': 'indicador'})
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1110 non-null   object 
#   1   geonombreFundar  1110 non-null   object 
#   2   indicador        1110 non-null   object 
#   3   valor            1107 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | indicador        |    valor |
#  |---:|:------------------|:------------------|:-----------------|---------:|
#  |  0 | AFG               | Afganistán        | pescado_mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='pescado_mariscos', new_value='Pescados y mariscos')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1110 non-null   object 
#   1   geonombreFundar  1110 non-null   object 
#   2   indicador        1110 non-null   object 
#   3   valor            1107 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | indicador           |    valor |
#  |---:|:------------------|:------------------|:--------------------|---------:|
#  |  0 | AFG               | Afganistán        | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='otras_carnes', new_value='Otras carnes')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1110 non-null   object 
#   1   geonombreFundar  1110 non-null   object 
#   2   indicador        1110 non-null   object 
#   3   valor            1107 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | indicador           |    valor |
#  |---:|:------------------|:------------------|:--------------------|---------:|
#  |  0 | AFG               | Afganistán        | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='aviar', new_value='Aviar')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1110 non-null   object 
#   1   geonombreFundar  1110 non-null   object 
#   2   indicador        1110 non-null   object 
#   3   valor            1107 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | indicador           |    valor |
#  |---:|:------------------|:------------------|:--------------------|---------:|
#  |  0 | AFG               | Afganistán        | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='caprina_ovina', new_value='Caprina y ovina')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1110 non-null   object 
#   1   geonombreFundar  1110 non-null   object 
#   2   indicador        1110 non-null   object 
#   3   valor            1107 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | indicador           |    valor |
#  |---:|:------------------|:------------------|:--------------------|---------:|
#  |  0 | AFG               | Afganistán        | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='porcina', new_value='Porcina')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1110 non-null   object 
#   1   geonombreFundar  1110 non-null   object 
#   2   indicador        1110 non-null   object 
#   3   valor            1107 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | indicador           |    valor |
#  |---:|:------------------|:------------------|:--------------------|---------:|
#  |  0 | AFG               | Afganistán        | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='vacuna', new_value='Vacuna')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1110 non-null   object 
#   1   geonombreFundar  1110 non-null   object 
#   2   indicador        1110 non-null   object 
#   3   valor            1107 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | indicador           |    valor |
#  |---:|:------------------|:------------------|:--------------------|---------:|
#  |  0 | AFG               | Afganistán        | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  query(condition="geocodigoFundar != 'F351'")
#  Index: 1110 entries, 0 to 1109
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  1110 non-null   object  
#   1   geonombreFundar  1110 non-null   category
#   2   indicador        1110 non-null   category
#   3   valor            1107 non-null   float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   | indicador           |    valor |
#  |---:|:------------------|:------------------|:--------------------|---------:|
#  |  0 | AFG               | Afganistán        | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geonombreFundar', order1=['Argentina', 'Zimbabwe', 'Estados Unidos', 'Australia', 'Brasil', 'Uzbekistán', 'Canadá', 'Chad', 'Israel', 'Mongolia', 'Kazajstán', 'Tayikistán', 'Luxemburgo', 'Armenia', 'Dinamarca', 'Paraguay', 'Chile', 'Malta', 'Hong Kong', 'Suecia', 'Turkmenistán', 'Bolivia', 'Francia', 'Polinesia Francesa', 'Irlanda', 'Uruguay', 'Suiza', 'Rep. Centroafricana', 'Portugal', 'Finlandia', 'Belarús', 'Noruega', 'Nueva Zelanda', 'Nueva Caledonia', 'Reino Unido', 'Sudáfrica', 'Nauru', 'Corea del Sur', 'Italia', 'Turquía', 'Países Bajos', 'Eswatini', 'Bahrein', 'Kirguistán', 'Austria', 'Eslovenia', 'Montenegro', 'México', 'Alemania', 'Panamá', 'Grecia', 'Bélgica', 'Colombia', 'Ecuador', 'Azerbaiyán', 'Sudán del Sur', 'Islandia', 'Rusia', 'Bosnia y Herzegovina', 'España', 'Guatemala', 'Venezuela', 'Albania', 'Costa Rica', 'Croacia', 'Líbano', 'Chequia', 'Omán', 'Japón', 'Pakistán', 'Myanmar', 'Botswana', 'Bhután', 'Kuwait', 'Zambia', 'Macao', 'Namibia', 'Qatar', 'Seychelles', 'Estonia', 'El Salvador', 'Laos', 'San Vicente y las Granadinas', 'Barbados', 'Guinea', 'Nepal', 'Dominica', 'Sudán', 'Marruecos', 'Tanzanía', 'Cuba', 'Irán', 'Serbia', 'Ucrania', 'Emiratos Árabes Unidos', 'Vanuatu', 'Egipto', 'Macedonia del Norte', 'China', 'Mauritania', 'Samoa', 'Jordania', 'Maldivas', 'Rep. Dominicana', 'Vietnam', 'Georgia', 'Taiwán', 'Honduras', 'Malasia', 'Eslovaquia', 'Chipre', 'Lituania', 'Djibouti', 'Antigua y Barbuda', 'Letonia', 'Trinidad y Tobago', 'Rumania', 'Burkina Faso', 'Senegal', 'Bahamas', 'Micronesia', 'Kenia', 'Gabón', 'Hungría', 'Mauricio', 'Haití', 'Camboya', 'Arabia Saudita', 'Suriname', 'Perú', 'Guyana', 'Jamaica', 'Argelia', 'Santa Lucía', 'Granada', 'Etiopía', 'Uganda', 'Belice', 'Guinea-Bissau', 'Túnez', 'Benin', 'Bulgaria', 'Malí', 'Angola', 'Saint Kitts y Nevis', 'Comoras', 'Afganistán', 'Gambia', 'Camerún', 'Fiji', 'Filipinas', 'Libia', 'Malawi', 'Níger', 'Indonesia', 'Yemen', 'Ruanda', 'Moldavia', 'Iraq', 'Nicaragua', 'Congo', 'Lesotho', 'Kiribati', 'Siria', 'Islas Salomón', 'Santo Tomé y Príncipe', 'Cabo Verde', 'Timor-Leste', 'Nigeria', 'Polonia', 'Ghana', 'Costa de Marfil', 'Madagascar', 'Bangladesh', 'Sri Lanka', 'Sierra Leona', 'Tailandia', 'India', 'Burundi', 'Corea del Norte', 'Togo', 'Papúa Nueva Guinea', 'Mozambique', 'Liberia', 'Rep. Dem. Congo'], col2='indicador', order2=['Vacuna', 'Porcina', 'Aviar', 'Pescados y mariscos', 'Caprina y ovina', 'Otras carnes'])
#  Index: 1110 entries, 28 to 203
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  1110 non-null   object  
#   1   geonombreFundar  1110 non-null   category
#   2   indicador        1110 non-null   category
#   3   valor            1107 non-null   float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   | indicador   |   valor |
#  |---:|:------------------|:------------------|:------------|--------:|
#  | 28 | ARG               | Argentina         | Vacuna      | 47.0965 |
#  
#  ------------------------------
#  