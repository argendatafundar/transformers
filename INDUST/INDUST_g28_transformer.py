from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def complete_cases(df: DataFrame, key_cols: list[str], n: int) -> DataFrame:
    """
    Devuelve únicamente las filas pertenecientes a aquellos grupos definidos
    por `key_cols` cuyo tamaño es exactamente `n`. Funciona tanto para
    una sola columna como para múltiples columnas.
    """

    # Tamaño de grupos
    group_sizes = df.groupby(key_cols).size()

    # Convertir el índice del groupby a tuplas (robusto para 1 o más columnas)
    valid_groups = group_sizes[group_sizes == n].index
    valid_groups = set(
        (g,) if not isinstance(g, tuple) else g
        for g in valid_groups
    )

    # Convertir las filas a tuplas de key_cols
    row_keys = df[key_cols].apply(
        lambda row: tuple(row) if len(key_cols) > 1 else (row.iloc[0],),
        axis=1
    )

    # Filtrar
    result = df[row_keys.isin(valid_groups)].copy()

    return result.reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio in [2011,2023]'),
	query(condition='clasificacion_lall == "Manufacturas de media o alta tecnología"'),
	complete_cases(key_cols=['exporter_iso3', 'geonombreFundar'], n=2),
	query(condition="geonombreFundar in ['Japón', 'México','Tailandia',  'Alemania', 'Vietnam', 'Francia', 'Corea del Sur', 'Italia', 'China', 'España', 'Brasil', 'Estados Unidos', 'India', 'Países Bajos', 'Paraguay']")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 11151 entries, 0 to 11150
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                11151 non-null  int64  
#   1   exporter_iso3       11151 non-null  object 
#   2   clasificacion_lall  11151 non-null  object 
#   3   impo                11151 non-null  float64
#   4   geonombreFundar     11151 non-null  object 
#   5   prop                11151 non-null  float64
#  
#  |    |   anio | exporter_iso3   | clasificacion_lall              |   impo | geonombreFundar   |    prop |
#  |---:|-------:|:----------------|:--------------------------------|-------:|:------------------|--------:|
#  |  0 |   2002 | AND             | Manufacturas de baja tecnología | 11.367 | Andorra           | 80.9039 |
#  
#  ------------------------------
#  
#  query(condition='anio in [2011,2023]')
#  Index: 1051 entries, 4216 to 11150
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                1051 non-null   int64  
#   1   exporter_iso3       1051 non-null   object 
#   2   clasificacion_lall  1051 non-null   object 
#   3   impo                1051 non-null   float64
#   4   geonombreFundar     1051 non-null   object 
#   5   prop                1051 non-null   float64
#  
#  |      |   anio | exporter_iso3   | clasificacion_lall              |   impo | geonombreFundar   |   prop |
#  |-----:|-------:|:----------------|:--------------------------------|-------:|:------------------|-------:|
#  | 4216 |   2011 | ABW             | Manufacturas en basadas en RRNN |  24078 | Aruba             |    100 |
#  
#  ------------------------------
#  
#  query(condition='clasificacion_lall == "Manufacturas de media o alta tecnología"')
#  Index: 303 entries, 4218 to 11148
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                303 non-null    int64  
#   1   exporter_iso3       303 non-null    object 
#   2   clasificacion_lall  303 non-null    object 
#   3   impo                303 non-null    float64
#   4   geonombreFundar     303 non-null    object 
#   5   prop                303 non-null    float64
#  
#  |      |   anio | exporter_iso3   | clasificacion_lall                      |   impo | geonombreFundar   |    prop |
#  |-----:|-------:|:----------------|:----------------------------------------|-------:|:------------------|--------:|
#  | 4218 |   2011 | AFG             | Manufacturas de media o alta tecnología |  5.269 | Afganistán        | 63.3903 |
#  
#  ------------------------------
#  
#  complete_cases(key_cols=['exporter_iso3', 'geonombreFundar'], n=2)
#  RangeIndex: 274 entries, 0 to 273
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                274 non-null    int64  
#   1   exporter_iso3       274 non-null    object 
#   2   clasificacion_lall  274 non-null    object 
#   3   impo                274 non-null    float64
#   4   geonombreFundar     274 non-null    object 
#   5   prop                274 non-null    float64
#  
#  |    |   anio | exporter_iso3   | clasificacion_lall                      |   impo | geonombreFundar   |    prop |
#  |---:|-------:|:----------------|:----------------------------------------|-------:|:------------------|--------:|
#  |  0 |   2011 | AFG             | Manufacturas de media o alta tecnología |  5.269 | Afganistán        | 63.3903 |
#  
#  ------------------------------
#  
#  query(condition="geonombreFundar in ['Japón', 'México','Tailandia',  'Alemania', 'Vietnam', 'Francia', 'Corea del Sur', 'Italia', 'China', 'España', 'Brasil', 'Estados Unidos', 'India', 'Países Bajos', 'Paraguay']")
#  Index: 30 entries, 17 to 269
#  Data columns (total 6 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   anio                30 non-null     int64  
#   1   exporter_iso3       30 non-null     object 
#   2   clasificacion_lall  30 non-null     object 
#   3   impo                30 non-null     float64
#   4   geonombreFundar     30 non-null     object 
#   5   prop                30 non-null     float64
#  
#  |    |   anio | exporter_iso3   | clasificacion_lall                      |        impo | geonombreFundar   |    prop |
#  |---:|-------:|:----------------|:----------------------------------------|------------:|:------------------|--------:|
#  | 17 |   2011 | BRA             | Manufacturas de media o alta tecnología | 1.53346e+07 | Brasil            | 68.9688 |
#  
#  ------------------------------
#  