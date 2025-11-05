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
	ordenar_dos_columnas(col1='geonombreFundar', order1=['Nigeria', 'Bolivia', 'Croacia', 'Andorra', 'Mongolia', 'Kazajstán', 'Turquía', 'Irán', 'España', 'Brasil', 'Iraq', 'Polonia', 'Pakistán', 'Kenia', 'Perú', 'Vietnam', 'Bangladesh', 'Etiopía', 'Venezuela', 'Tailandia', 'Guatemala', 'Tayikistán', 'Uruguay', 'Rumania', 'Georgia', 'Colombia', 'Rusia', 'Indonesia', 'Libia', 'Maldivas', 'Malasia', 'China', 'Macedonia del Norte', 'Albania', 'Bosnia y Herzegovina', 'Zimbabwe', 'Portugal', 'Nicaragua', 'Ecuador', 'Armenia', 'Egipto', 'Serbia', 'Bulgaria', 'Macao', 'Myanmar', 'Puerto Rico', 'Islandia', 'Eslovenia', 'Túnez', 'Argentina', 'Singapur', 'Marruecos', 'Ucrania', 'Corea del Sur', 'Alemania', 'Belarús', 'Estados Unidos', 'Chile', 'Irlanda del Norte', 'Hong Kong', 'Italia', 'Grecia', 'Austria', 'Jordania', 'Líbano', 'Francia', 'Hungría', 'Países Bajos', 'Dinamarca', 'Finlandia', 'Suiza', 'Taiwán', 'Letonia', 'Reino Unido', 'Lituania', 'Eslovaquia', 'Estonia', 'Nueva Zelanda', 'Canadá', 'Australia', 'Japón', 'Chequia', 'Noruega', 'Suecia'], col2='estado', order2=['Desocupado', 'Ocupado'])
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
#  ordenar_dos_columnas(col1='geonombreFundar', order1=['Nigeria', 'Bolivia', 'Croacia', 'Andorra', 'Mongolia', 'Kazajstán', 'Turquía', 'Irán', 'España', 'Brasil', 'Iraq', 'Polonia', 'Pakistán', 'Kenia', 'Perú', 'Vietnam', 'Bangladesh', 'Etiopía', 'Venezuela', 'Tailandia', 'Guatemala', 'Tayikistán', 'Uruguay', 'Rumania', 'Georgia', 'Colombia', 'Rusia', 'Indonesia', 'Libia', 'Maldivas', 'Malasia', 'China', 'Macedonia del Norte', 'Albania', 'Bosnia y Herzegovina', 'Zimbabwe', 'Portugal', 'Nicaragua', 'Ecuador', 'Armenia', 'Egipto', 'Serbia', 'Bulgaria', 'Macao', 'Myanmar', 'Puerto Rico', 'Islandia', 'Eslovenia', 'Túnez', 'Argentina', 'Singapur', 'Marruecos', 'Ucrania', 'Corea del Sur', 'Alemania', 'Belarús', 'Estados Unidos', 'Chile', 'Irlanda del Norte', 'Hong Kong', 'Italia', 'Grecia', 'Austria', 'Jordania', 'Líbano', 'Francia', 'Hungría', 'Países Bajos', 'Dinamarca', 'Finlandia', 'Suiza', 'Taiwán', 'Letonia', 'Reino Unido', 'Lituania', 'Eslovaquia', 'Estonia', 'Nueva Zelanda', 'Canadá', 'Australia', 'Japón', 'Chequia', 'Noruega', 'Suecia'], col2='estado', order2=['Desocupado', 'Ocupado'])
#  Index: 168 entries, 25 to 87
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
#  | 25 | NGA               | Nigeria           |   2018 | Desocupado |              5.5686 |
#  
#  ------------------------------
#  