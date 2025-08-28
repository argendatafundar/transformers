from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df

@transformer.convert
def map_categoria(df:DataFrame, curr_col:str, new_col:str, mapper:dict)->DataFrame:
    df[new_col] = df[curr_col].apply(lambda x: mapper.get(x, None))
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='region', curr_value='América central', new_value='América Central', mapping=None),
	map_categoria(curr_col='geonombreFundar', new_col='destacado', mapper={'Albania': 'Otros', 'Argelia': 'Otros', 'Angola': 'Otros', 'Antigua y Barbuda': 'Otros', 'Argentina': 'Argentina', 'Aruba': 'Otros', 'Australia': 'Otros', 'Austria': 'Otros', 'Azerbaiyán': 'Otros', 'Bahamas': 'Otros', 'Bahrein': 'Otros', 'Bangladesh': 'Otros', 'Barbados': 'Otros', 'Bélgica': 'Otros', 'Belice': 'Otros', 'Benin': 'Otros', 'Bolivia': 'Otros', 'Bosnia y Herzegovina': 'Otros', 'Brasil': 'Otros', 'Bulgaria': 'Otros', 'Burkina Faso': 'Otros', 'Burundi': 'Otros', 'Cabo Verde': 'Otros', 'Camboya': 'Otros', 'Canadá': 'Otros', 'Rep. Centroafricana': 'Otros', 'Chad': 'Otros', 'Chile': 'Otros', 'China': 'Otros', 'Colombia': 'Otros', 'Comoras': 'Otros', 'Rep. Dem. Congo': 'Otros', 'Congo': 'Otros', 'Costa Rica': 'Otros', 'Costa de Marfil': 'Otros', 'Croacia': 'Otros', 'Chipre': 'Otros', 'Chequia': 'Otros', 'Dinamarca': 'Otros', 'Djibouti': 'Otros', 'Dominica': 'Otros', 'Rep. Dominicana': 'Otros', 'Ecuador': 'Otros', 'El Salvador': 'Otros', 'Guinea Ecuatorial': 'Otros', 'Estonia': 'Otros', 'Eswatini': 'Otros', 'Etiopía': 'Otros', 'Fiji': 'Otros', 'Finlandia': 'Otros', 'Francia': 'Otros', 'Gabón': 'Otros', 'Georgia': 'Otros', 'Alemania': 'Otros', 'Ghana': 'Otros', 'Grecia': 'Otros', 'Granada': 'Otros', 'Guatemala': 'Otros', 'Guinea': 'Otros', 'Guinea-Bissau': 'Otros', 'Guyana': 'Otros', 'Haití': 'Otros', 'Honduras': 'Otros', 'Hong Kong': 'Otros', 'Hungría': 'Otros', 'Islandia': 'Otros', 'India': 'Otros', 'Indonesia': 'Otros', 'Irán': 'Otros', 'Irlanda': 'Otros', 'Israel': 'Otros', 'Italia': 'Otros', 'Jamaica': 'Otros', 'Japón': 'Otros', 'Jordania': 'Otros', 'Kenia': 'Otros', 'Kiribati': 'Otros', 'Corea del Sur': 'Otros', 'Kuwait': 'Otros', 'Kirguistán': 'Otros', 'Letonia': 'Otros', 'Lesotho': 'Otros', 'Luxemburgo': 'Otros', 'Madagascar': 'Otros', 'Malasia': 'Otros', 'Maldivas': 'Otros', 'Islas Marshall': 'Otros', 'México': 'Otros', 'Micronesia': 'Otros', 'Moldavia': 'Otros', 'Mongolia': 'Otros', 'Marruecos': 'Otros', 'Mozambique': 'Otros', 'Myanmar': 'Otros', 'Namibia': 'Otros', 'Países Bajos': 'Otros', 'Nueva Zelanda': 'Otros', 'Nicaragua': 'Otros', 'Níger': 'Otros', 'Nigeria': 'Otros', 'Macedonia del Norte': 'Otros', 'Noruega': 'Otros', 'Omán': 'Otros', 'Pakistán': 'Otros', 'Panamá': 'Otros', 'Papúa Nueva Guinea': 'Otros', 'Paraguay': 'Otros', 'Perú': 'Otros', 'Filipinas': 'Otros', 'Polonia': 'Otros', 'Portugal': 'Otros', 'Qatar': 'Otros', 'Rumania': 'Otros', 'Rusia': 'Otros', 'Ruanda': 'Otros', 'Arabia Saudita': 'Otros', 'Senegal': 'Otros', 'Seychelles': 'Otros', 'Eslovaquia': 'Otros', 'Eslovenia': 'Otros', 'Islas Salomón': 'Otros', 'Sudáfrica': 'Otros', 'España': 'Otros', 'Saint Kitts y Nevis': 'Otros', 'Santa Lucía': 'Otros', 'San Vicente y las Granadinas': 'Otros', 'Sudán': 'Otros', 'Suriname': 'Otros', 'Suecia': 'Otros', 'Suiza': 'Otros', 'Tayikistán': 'Otros', 'Tanzanía': 'Otros', 'Tailandia': 'Otros', 'Togo': 'Otros', 'Trinidad y Tobago': 'Otros', 'Túnez': 'Otros', 'Turquía': 'Otros', 'Uganda': 'Otros', 'Ucrania': 'Otros', 'Emiratos Árabes Unidos': 'Otros', 'Reino Unido': 'Otros', 'Estados Unidos': 'Otros', 'Uruguay': 'Otros', 'Uzbekistán': 'Otros', 'Vanuatu': 'Otros', 'Vietnam': 'Otros'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 292 entries, 0 to 291
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  292 non-null    object 
#   1   anio             292 non-null    int64  
#   2   geonombreFundar  292 non-null    object 
#   3   region           292 non-null    object 
#   4   variable         292 non-null    object 
#   5   valor            292 non-null    float64
#  
#  |    | geocodigoFundar   |   anio | geonombreFundar   | region   | variable                                                           |   valor |
#  |---:|:------------------|-------:|:------------------|:---------|:-------------------------------------------------------------------|--------:|
#  |  0 | ALB               |   2023 | Albania           | Europa   | PIB per cápita PPP (en dólares internacionales constantes de 2021) | 17992.1 |
#  
#  ------------------------------
#  
#  replace_value(col='region', curr_value='América central', new_value='América Central', mapping=None)
#  RangeIndex: 292 entries, 0 to 291
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  292 non-null    object 
#   1   anio             292 non-null    int64  
#   2   geonombreFundar  292 non-null    object 
#   3   region           292 non-null    object 
#   4   variable         292 non-null    object 
#   5   valor            292 non-null    float64
#   6   destacado        292 non-null    object 
#  
#  |    | geocodigoFundar   |   anio | geonombreFundar   | region   | variable                                                           |   valor | destacado   |
#  |---:|:------------------|-------:|:------------------|:---------|:-------------------------------------------------------------------|--------:|:------------|
#  |  0 | ALB               |   2023 | Albania           | Europa   | PIB per cápita PPP (en dólares internacionales constantes de 2021) | 17992.1 | Otros       |
#  
#  ------------------------------
#  
#  map_categoria(curr_col='geonombreFundar', new_col='destacado', mapper={'Albania': 'Otros', 'Argelia': 'Otros', 'Angola': 'Otros', 'Antigua y Barbuda': 'Otros', 'Argentina': 'Argentina', 'Aruba': 'Otros', 'Australia': 'Otros', 'Austria': 'Otros', 'Azerbaiyán': 'Otros', 'Bahamas': 'Otros', 'Bahrein': 'Otros', 'Bangladesh': 'Otros', 'Barbados': 'Otros', 'Bélgica': 'Otros', 'Belice': 'Otros', 'Benin': 'Otros', 'Bolivia': 'Otros', 'Bosnia y Herzegovina': 'Otros', 'Brasil': 'Otros', 'Bulgaria': 'Otros', 'Burkina Faso': 'Otros', 'Burundi': 'Otros', 'Cabo Verde': 'Otros', 'Camboya': 'Otros', 'Canadá': 'Otros', 'Rep. Centroafricana': 'Otros', 'Chad': 'Otros', 'Chile': 'Otros', 'China': 'Otros', 'Colombia': 'Otros', 'Comoras': 'Otros', 'Rep. Dem. Congo': 'Otros', 'Congo': 'Otros', 'Costa Rica': 'Otros', 'Costa de Marfil': 'Otros', 'Croacia': 'Otros', 'Chipre': 'Otros', 'Chequia': 'Otros', 'Dinamarca': 'Otros', 'Djibouti': 'Otros', 'Dominica': 'Otros', 'Rep. Dominicana': 'Otros', 'Ecuador': 'Otros', 'El Salvador': 'Otros', 'Guinea Ecuatorial': 'Otros', 'Estonia': 'Otros', 'Eswatini': 'Otros', 'Etiopía': 'Otros', 'Fiji': 'Otros', 'Finlandia': 'Otros', 'Francia': 'Otros', 'Gabón': 'Otros', 'Georgia': 'Otros', 'Alemania': 'Otros', 'Ghana': 'Otros', 'Grecia': 'Otros', 'Granada': 'Otros', 'Guatemala': 'Otros', 'Guinea': 'Otros', 'Guinea-Bissau': 'Otros', 'Guyana': 'Otros', 'Haití': 'Otros', 'Honduras': 'Otros', 'Hong Kong': 'Otros', 'Hungría': 'Otros', 'Islandia': 'Otros', 'India': 'Otros', 'Indonesia': 'Otros', 'Irán': 'Otros', 'Irlanda': 'Otros', 'Israel': 'Otros', 'Italia': 'Otros', 'Jamaica': 'Otros', 'Japón': 'Otros', 'Jordania': 'Otros', 'Kenia': 'Otros', 'Kiribati': 'Otros', 'Corea del Sur': 'Otros', 'Kuwait': 'Otros', 'Kirguistán': 'Otros', 'Letonia': 'Otros', 'Lesotho': 'Otros', 'Luxemburgo': 'Otros', 'Madagascar': 'Otros', 'Malasia': 'Otros', 'Maldivas': 'Otros', 'Islas Marshall': 'Otros', 'México': 'Otros', 'Micronesia': 'Otros', 'Moldavia': 'Otros', 'Mongolia': 'Otros', 'Marruecos': 'Otros', 'Mozambique': 'Otros', 'Myanmar': 'Otros', 'Namibia': 'Otros', 'Países Bajos': 'Otros', 'Nueva Zelanda': 'Otros', 'Nicaragua': 'Otros', 'Níger': 'Otros', 'Nigeria': 'Otros', 'Macedonia del Norte': 'Otros', 'Noruega': 'Otros', 'Omán': 'Otros', 'Pakistán': 'Otros', 'Panamá': 'Otros', 'Papúa Nueva Guinea': 'Otros', 'Paraguay': 'Otros', 'Perú': 'Otros', 'Filipinas': 'Otros', 'Polonia': 'Otros', 'Portugal': 'Otros', 'Qatar': 'Otros', 'Rumania': 'Otros', 'Rusia': 'Otros', 'Ruanda': 'Otros', 'Arabia Saudita': 'Otros', 'Senegal': 'Otros', 'Seychelles': 'Otros', 'Eslovaquia': 'Otros', 'Eslovenia': 'Otros', 'Islas Salomón': 'Otros', 'Sudáfrica': 'Otros', 'España': 'Otros', 'Saint Kitts y Nevis': 'Otros', 'Santa Lucía': 'Otros', 'San Vicente y las Granadinas': 'Otros', 'Sudán': 'Otros', 'Suriname': 'Otros', 'Suecia': 'Otros', 'Suiza': 'Otros', 'Tayikistán': 'Otros', 'Tanzanía': 'Otros', 'Tailandia': 'Otros', 'Togo': 'Otros', 'Trinidad y Tobago': 'Otros', 'Túnez': 'Otros', 'Turquía': 'Otros', 'Uganda': 'Otros', 'Ucrania': 'Otros', 'Emiratos Árabes Unidos': 'Otros', 'Reino Unido': 'Otros', 'Estados Unidos': 'Otros', 'Uruguay': 'Otros', 'Uzbekistán': 'Otros', 'Vanuatu': 'Otros', 'Vietnam': 'Otros'})
#  RangeIndex: 292 entries, 0 to 291
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  292 non-null    object 
#   1   anio             292 non-null    int64  
#   2   geonombreFundar  292 non-null    object 
#   3   region           292 non-null    object 
#   4   variable         292 non-null    object 
#   5   valor            292 non-null    float64
#   6   destacado        292 non-null    object 
#  
#  |    | geocodigoFundar   |   anio | geonombreFundar   | region   | variable                                                           |   valor | destacado   |
#  |---:|:------------------|-------:|:------------------|:---------|:-------------------------------------------------------------------|--------:|:------------|
#  |  0 | ALB               |   2023 | Albania           | Europa   | PIB per cápita PPP (en dólares internacionales constantes de 2021) | 17992.1 | Otros       |
#  
#  ------------------------------
#  