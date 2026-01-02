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
	query(condition='anio == anio.max()'),
	complete_cases(key_cols=['exporter_iso3', 'geonombreFundar'], n=2),
	query(condition="geonombreFundar in ['Brasil', 'Estados Unidos', 'India', 'China', 'Tailandia', 'Italia', 'Países Bajos', 'Vietnam', 'España', 'México', 'Corea del Sur', 'Alemania', 'Francia', 'Paraguay']")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 5907 entries, 0 to 5906
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             5907 non-null   int64  
#   1   exporter_iso3    5907 non-null   object 
#   2   geonombreFundar  5863 non-null   object 
#   3   tipo_bien        5907 non-null   object 
#   4   impo             5907 non-null   float64
#   5   prop             5907 non-null   float64
#  
#  |    |   anio | exporter_iso3   | geonombreFundar   | tipo_bien   |   impo |     prop |
#  |---:|-------:|:----------------|:------------------|:------------|-------:|---------:|
#  |  0 |   2002 | AUT             | Austria           | Primarios   | 600.08 | 0.103429 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 271 entries, 1206 to 5906
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             271 non-null    int64  
#   1   exporter_iso3    271 non-null    object 
#   2   geonombreFundar  269 non-null    object 
#   3   tipo_bien        271 non-null    object 
#   4   impo             271 non-null    float64
#   5   prop             271 non-null    float64
#  
#  |      |   anio | exporter_iso3   | geonombreFundar   | tipo_bien   |    impo |     prop |
#  |-----:|-------:|:----------------|:------------------|:------------|--------:|---------:|
#  | 1206 |   2023 | BEL             | Bélgica           | Primarios   | 19333.3 | 0.171252 |
#  
#  ------------------------------
#  
#  complete_cases(key_cols=['exporter_iso3', 'geonombreFundar'], n=2)
#  RangeIndex: 192 entries, 0 to 191
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             192 non-null    int64  
#   1   exporter_iso3    192 non-null    object 
#   2   geonombreFundar  192 non-null    object 
#   3   tipo_bien        192 non-null    object 
#   4   impo             192 non-null    float64
#   5   prop             192 non-null    float64
#  
#  |    |   anio | exporter_iso3   | geonombreFundar   | tipo_bien   |    impo |     prop |
#  |---:|-------:|:----------------|:------------------|:------------|--------:|---------:|
#  |  0 |   2023 | BEL             | Bélgica           | Primarios   | 19333.3 | 0.171252 |
#  
#  ------------------------------
#  
#  query(condition="geonombreFundar in ['Brasil', 'Estados Unidos', 'India', 'China', 'Tailandia', 'Italia', 'Países Bajos', 'Vietnam', 'España', 'México', 'Corea del Sur', 'Alemania', 'Francia', 'Paraguay']")
#  Index: 28 entries, 6 to 175
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             28 non-null     int64  
#   1   exporter_iso3    28 non-null     object 
#   2   geonombreFundar  28 non-null     object 
#   3   tipo_bien        28 non-null     object 
#   4   impo             28 non-null     float64
#   5   prop             28 non-null     float64
#  
#  |    |   anio | exporter_iso3   | geonombreFundar   | tipo_bien   |    impo |    prop |
#  |---:|-------:|:----------------|:------------------|:------------|--------:|--------:|
#  |  6 |   2023 | DEU             | Alemania          | Primarios   | 37504.5 | 0.33221 |
#  
#  ------------------------------
#  