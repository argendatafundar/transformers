from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def filtrar_por_max_anio(df, group_cols):
    idx = df.groupby(group_cols)['anio'].transform('max') == df['anio']
    return df[idx]

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
	query(condition="geonombreFundar not in ['Kirguistán','Filipinas','Montenegro','Chipre','México','Azerbaiyán']"),
	filtrar_por_max_anio(group_cols=['geonombreFundar', 'estado']),
	sort_values(how='ascending', by=['estado']),
	ordenar_dos_columnas(col1='geonombreFundar', order1=['Suecia', 'Noruega', 'Chequia', 'Japón', 'Australia', 'Canadá', 'Nueva Zelanda', 'Estonia', 'Eslovaquia', 'Lituania', 'Reino Unido', 'Letonia', 'Taiwán', 'Suiza', 'Finlandia', 'Dinamarca', 'Países Bajos', 'Hungría', 'Francia', 'Líbano', 'Jordania', 'Austria', 'Grecia', 'Italia', 'Hong Kong', 'Irlanda del Norte', 'Chile', 'Estados Unidos', 'Belarús', 'Alemania', 'Corea del Sur', 'Ucrania', 'Marruecos', 'Singapur', 'Argentina', 'Túnez', 'Eslovenia', 'Islandia', 'Puerto Rico', 'Myanmar', 'Macao', 'Bulgaria', 'Serbia', 'Egipto', 'Armenia', 'Ecuador', 'Nicaragua', 'Portugal', 'Zimbabwe', 'Bosnia y Herzegovina', 'Albania', 'Macedonia del Norte', 'China', 'Malasia', 'Maldivas', 'Libia', 'Indonesia', 'Rusia', 'Colombia', 'Georgia', 'Rumania', 'Uruguay', 'Tayikistán', 'Guatemala', 'Tailandia', 'Venezuela', 'Etiopía', 'Bangladesh', 'Vietnam', 'Perú', 'Kenia', 'Pakistán', 'Polonia', 'Iraq', 'Brasil', 'España', 'Irán', 'Turquía', 'Kazajstán', 'Mongolia', 'Andorra', 'Croacia', 'Bolivia', 'Nigeria'], col2='estado', order2=['Desocupado', 'Ocupado'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 193 entries, 0 to 192
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    193 non-null    object 
#   1   geonombreFundar    193 non-null    object 
#   2   anio               193 non-null    int64  
#   3   estado             193 non-null    object 
#   4   satisfaccion_vida  193 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | estado     |   satisfaccion_vida |
#  |---:|:------------------|:------------------|-------:|:-----------|--------------------:|
#  |  0 | ALB               | Albania           |   2018 | Desocupado |               6.642 |
#  
#  ------------------------------
#  
#  query(condition="geonombreFundar not in ['Kirguistán','Filipinas','Montenegro','Chipre','México','Azerbaiyán']")
#  Index: 182 entries, 0 to 192
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    182 non-null    object 
#   1   geonombreFundar    182 non-null    object 
#   2   anio               182 non-null    int64  
#   3   estado             182 non-null    object 
#   4   satisfaccion_vida  182 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | estado     |   satisfaccion_vida |
#  |---:|:------------------|:------------------|-------:|:-----------|--------------------:|
#  |  0 | ALB               | Albania           |   2018 | Desocupado |               6.642 |
#  
#  ------------------------------
#  
#  filtrar_por_max_anio(group_cols=['geonombreFundar', 'estado'])
#  Index: 168 entries, 0 to 192
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    168 non-null    object 
#   1   geonombreFundar    168 non-null    object 
#   2   anio               168 non-null    int64  
#   3   estado             168 non-null    object 
#   4   satisfaccion_vida  168 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | estado     |   satisfaccion_vida |
#  |---:|:------------------|:------------------|-------:|:-----------|--------------------:|
#  |  0 | ALB               | Albania           |   2018 | Desocupado |               6.642 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['estado'])
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype   
#  ---  ------             --------------  -----   
#   0   geocodigoFundar    168 non-null    object  
#   1   geonombreFundar    168 non-null    category
#   2   anio               168 non-null    int64   
#   3   estado             168 non-null    category
#   4   satisfaccion_vida  168 non-null    float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | estado     |   satisfaccion_vida |
#  |---:|:------------------|:------------------|-------:|:-----------|--------------------:|
#  |  0 | ALB               | Albania           |   2018 | Desocupado |               6.642 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geonombreFundar', order1=['Suecia', 'Noruega', 'Chequia', 'Japón', 'Australia', 'Canadá', 'Nueva Zelanda', 'Estonia', 'Eslovaquia', 'Lituania', 'Reino Unido', 'Letonia', 'Taiwán', 'Suiza', 'Finlandia', 'Dinamarca', 'Países Bajos', 'Hungría', 'Francia', 'Líbano', 'Jordania', 'Austria', 'Grecia', 'Italia', 'Hong Kong', 'Irlanda del Norte', 'Chile', 'Estados Unidos', 'Belarús', 'Alemania', 'Corea del Sur', 'Ucrania', 'Marruecos', 'Singapur', 'Argentina', 'Túnez', 'Eslovenia', 'Islandia', 'Puerto Rico', 'Myanmar', 'Macao', 'Bulgaria', 'Serbia', 'Egipto', 'Armenia', 'Ecuador', 'Nicaragua', 'Portugal', 'Zimbabwe', 'Bosnia y Herzegovina', 'Albania', 'Macedonia del Norte', 'China', 'Malasia', 'Maldivas', 'Libia', 'Indonesia', 'Rusia', 'Colombia', 'Georgia', 'Rumania', 'Uruguay', 'Tayikistán', 'Guatemala', 'Tailandia', 'Venezuela', 'Etiopía', 'Bangladesh', 'Vietnam', 'Perú', 'Kenia', 'Pakistán', 'Polonia', 'Iraq', 'Brasil', 'España', 'Irán', 'Turquía', 'Kazajstán', 'Mongolia', 'Andorra', 'Croacia', 'Bolivia', 'Nigeria'], col2='estado', order2=['Desocupado', 'Ocupado'])
#  Index: 168 entries, 20 to 112
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype   
#  ---  ------             --------------  -----   
#   0   geocodigoFundar    168 non-null    object  
#   1   geonombreFundar    168 non-null    category
#   2   anio               168 non-null    int64   
#   3   estado             168 non-null    category
#   4   satisfaccion_vida  168 non-null    float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | estado     |   satisfaccion_vida |
#  |---:|:------------------|:------------------|-------:|:-----------|--------------------:|
#  | 20 | SWE               | Suecia            |   2017 | Desocupado |             5.56892 |
#  
#  ------------------------------
#  