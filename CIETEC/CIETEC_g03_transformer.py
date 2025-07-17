from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def fill_na(df:DataFrame, col:str, fill:Any):
    df[col] = df[col].fillna(fill)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def completar_combinaciones(df:DataFrame, keys:list[str]):

    import pandas as pd

    niveles = [df[key].dropna().unique() for key in keys]
    combinaciones = pd.MultiIndex.from_product(niveles, names=keys).to_frame(index=False)
    df = combinaciones.merge(df, on=keys, how='left')
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="geonombreFundar not in ['Ecuador','Honduras','Puerto Rico']"),
	drop_col(col=['ultimo_anio_disponible', 'fuente', 'geocodigoFundar'], axis=1),
	completar_combinaciones(keys=['geonombreFundar', 'sector']),
	fill_na(col='valor', fill=0),
	ordenar_dos_columnas(col1='geonombreFundar', order1=['Israel', 'Irlanda', 'Taiwán', 'Corea del Sur', 'Japón', 'Estados Unidos', 'China', 'Islandia', 'Suecia', 'Bélgica', 'Países miembros de OCDE', 'Hungría', 'Reino Unido', 'Países Bajos', 'Austria', 'Eslovenia', 'Suiza', 'Alemania', 'Finlandia', 'Unión Europea (27 países)', 'Francia', 'Turquía', 'Chequia', 'Polonia', 'Bulgaria', 'Singapur', 'Portugal', 'Rumania', 'Dinamarca', 'Canadá', 'Nueva Zelanda', 'Italia', 'Estonia', 'Noruega', 'Rusia', 'España', 'Eslovaquia', 'Croacia', 'Australia', 'Grecia', 'Uruguay', 'Luxemburgo', 'Colombia', 'Argentina', 'Chile', 'Lituania', 'Iberoamérica', 'Letonia', 'Sudáfrica', 'América Latina y el Caribe', 'Perú', 'Costa Rica', 'México', 'Panamá', 'Guatemala', 'Paraguay', 'Trinidad y Tobago'], col2='sector', order2=['Empresas (Públicas y Privadas)', 'Educación Superior', 'Gobierno', 'Org. priv. sin fines de lucro'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 230 entries, 0 to 229
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         230 non-null    object 
#   1   geonombreFundar         230 non-null    object 
#   2   ultimo_anio_disponible  230 non-null    int64  
#   3   sector                  230 non-null    object 
#   4   valor                   218 non-null    float64
#   5   fuente                  230 non-null    object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ultimo_anio_disponible | sector   |   valor | fuente   |
#  |---:|:------------------|:------------------|-------------------------:|:---------|--------:|:---------|
#  |  0 | BGR               | Bulgaria          |                     2023 | Gobierno | 28.7441 | OECD     |
#  
#  ------------------------------
#  
#  query(condition="geonombreFundar not in ['Ecuador','Honduras','Puerto Rico']")
#  Index: 218 entries, 0 to 229
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         218 non-null    object 
#   1   geonombreFundar         218 non-null    object 
#   2   ultimo_anio_disponible  218 non-null    int64  
#   3   sector                  218 non-null    object 
#   4   valor                   218 non-null    float64
#   5   fuente                  218 non-null    object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   ultimo_anio_disponible | sector   |   valor | fuente   |
#  |---:|:------------------|:------------------|-------------------------:|:---------|--------:|:---------|
#  |  0 | BGR               | Bulgaria          |                     2023 | Gobierno | 28.7441 | OECD     |
#  
#  ------------------------------
#  
#  drop_col(col=['ultimo_anio_disponible', 'fuente', 'geocodigoFundar'], axis=1)
#  Index: 218 entries, 0 to 229
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  218 non-null    object 
#   1   sector           218 non-null    object 
#   2   valor            218 non-null    float64
#  
#  |    | geonombreFundar   | sector   |   valor |
#  |---:|:------------------|:---------|--------:|
#  |  0 | Bulgaria          | Gobierno | 28.7441 |
#  
#  ------------------------------
#  
#  completar_combinaciones(keys=['geonombreFundar', 'sector'])
#  RangeIndex: 228 entries, 0 to 227
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  228 non-null    category
#   1   sector           228 non-null    category
#   2   valor            228 non-null    float64 
#  
#  |    | geonombreFundar   | sector   |   valor |
#  |---:|:------------------|:---------|--------:|
#  |  0 | Bulgaria          | Gobierno | 28.7441 |
#  
#  ------------------------------
#  
#  fill_na(col='valor', fill=0)
#  RangeIndex: 228 entries, 0 to 227
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  228 non-null    category
#   1   sector           228 non-null    category
#   2   valor            228 non-null    float64 
#  
#  |    | geonombreFundar   | sector   |   valor |
#  |---:|:------------------|:---------|--------:|
#  |  0 | Bulgaria          | Gobierno | 28.7441 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='geonombreFundar', order1=['Israel', 'Irlanda', 'Taiwán', 'Corea del Sur', 'Japón', 'Estados Unidos', 'China', 'Islandia', 'Suecia', 'Bélgica', 'Países miembros de OCDE', 'Hungría', 'Reino Unido', 'Países Bajos', 'Austria', 'Eslovenia', 'Suiza', 'Alemania', 'Finlandia', 'Unión Europea (27 países)', 'Francia', 'Turquía', 'Chequia', 'Polonia', 'Bulgaria', 'Singapur', 'Portugal', 'Rumania', 'Dinamarca', 'Canadá', 'Nueva Zelanda', 'Italia', 'Estonia', 'Noruega', 'Rusia', 'España', 'Eslovaquia', 'Croacia', 'Australia', 'Grecia', 'Uruguay', 'Luxemburgo', 'Colombia', 'Argentina', 'Chile', 'Lituania', 'Iberoamérica', 'Letonia', 'Sudáfrica', 'América Latina y el Caribe', 'Perú', 'Costa Rica', 'México', 'Panamá', 'Guatemala', 'Paraguay', 'Trinidad y Tobago'], col2='sector', order2=['Empresas (Públicas y Privadas)', 'Educación Superior', 'Gobierno', 'Org. priv. sin fines de lucro'])
#  Index: 228 entries, 63 to 221
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geonombreFundar  228 non-null    category
#   1   sector           228 non-null    category
#   2   valor            228 non-null    float64 
#  
#  |    | geonombreFundar   | sector                         |   valor |
#  |---:|:------------------|:-------------------------------|--------:|
#  | 63 | Israel            | Empresas (Públicas y Privadas) | 93.0072 |
#  
#  ------------------------------
#  