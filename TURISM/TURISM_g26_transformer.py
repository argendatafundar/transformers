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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	complete_cases(key_cols=['anio'], n=2)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 68 entries, 0 to 67
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             68 non-null     int64  
#   1   indicador        68 non-null     object 
#   2   expo_turisticas  68 non-null     float64
#  
#  |    |   anio | indicador   |   expo_turisticas |
#  |---:|-------:|:------------|------------------:|
#  |  0 |   1976 | Viajes      |               180 |
#  
#  ------------------------------
#  
#  complete_cases(key_cols=['anio'], n=2)
#  RangeIndex: 38 entries, 0 to 37
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             38 non-null     int64  
#   1   indicador        38 non-null     object 
#   2   expo_turisticas  38 non-null     float64
#  
#  |    |   anio | indicador   |   expo_turisticas |
#  |---:|-------:|:------------|------------------:|
#  |  0 |   2006 | Viajes      |           3351.27 |
#  
#  ------------------------------
#  