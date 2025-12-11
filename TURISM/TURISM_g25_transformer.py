from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio in [2000,2010,2024]'),
	complete_cases(key_cols=['geocodigoFundar'], n=3)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7505 entries, 0 to 7504
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             7505 non-null   int64  
#   1   geocodigoFundar  7505 non-null   object 
#   2   geonombreFundar  7505 non-null   object 
#   3   expo_turisticas  7505 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   expo_turisticas |
#  |---:|-------:|:------------------|:------------------|------------------:|
#  |  0 |   1986 | ABW               | Aruba             |           158.101 |
#  
#  ------------------------------
#  
#  query(condition='anio in [2000,2010,2024]')
#  Index: 484 entries, 14 to 7504
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             484 non-null    int64  
#   1   geocodigoFundar  484 non-null    object 
#   2   geonombreFundar  484 non-null    object 
#   3   expo_turisticas  484 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   expo_turisticas |
#  |---:|-------:|:------------------|:------------------|------------------:|
#  | 14 |   2000 | ABW               | Aruba             |           814.486 |
#  
#  ------------------------------
#  
#  complete_cases(key_cols=['geocodigoFundar'], n=3)
#  RangeIndex: 363 entries, 0 to 362
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             363 non-null    int64  
#   1   geocodigoFundar  363 non-null    object 
#   2   geonombreFundar  363 non-null    object 
#   3   expo_turisticas  363 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   expo_turisticas |
#  |---:|-------:|:------------------|:------------------|------------------:|
#  |  0 |   2000 | AGO               | Angola            |                 0 |
#  
#  ------------------------------
#  