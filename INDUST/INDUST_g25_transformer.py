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
	query(condition='anio == anio.max()'),
	complete_cases(key_cols=['importer_iso3', 'geonombreFundar'], n=2)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 7637 entries, 0 to 7636
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             7637 non-null   int64  
#   1   importer_iso3    7637 non-null   object 
#   2   geonombreFundar  7593 non-null   object 
#   3   tipo_bien        7637 non-null   object 
#   4   expo             7637 non-null   float64
#   5   prop             7637 non-null   float64
#  
#  |    |   anio | importer_iso3   | geonombreFundar   | tipo_bien    |   expo |        prop |
#  |---:|-------:|:----------------|:------------------|:-------------|-------:|------------:|
#  |  0 |   2002 | AFG             | Afganistán        | Manufacturas |  84.33 | 0.000595445 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 314 entries, 3637 to 7636
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             314 non-null    int64  
#   1   importer_iso3    314 non-null    object 
#   2   geonombreFundar  312 non-null    object 
#   3   tipo_bien        314 non-null    object 
#   4   expo             314 non-null    float64
#   5   prop             314 non-null    float64
#  
#  |      |   anio | importer_iso3   | geonombreFundar   | tipo_bien   |    expo |      prop |
#  |-----:|-------:|:----------------|:------------------|:------------|--------:|----------:|
#  | 3637 |   2023 | ALB             | Albania           | Primarios   | 7655.34 | 0.0200102 |
#  
#  ------------------------------
#  
#  complete_cases(key_cols=['importer_iso3', 'geonombreFundar'], n=2)
#  RangeIndex: 290 entries, 0 to 289
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             290 non-null    int64  
#   1   importer_iso3    290 non-null    object 
#   2   geonombreFundar  290 non-null    object 
#   3   tipo_bien        290 non-null    object 
#   4   expo             290 non-null    float64
#   5   prop             290 non-null    float64
#  
#  |    |   anio | importer_iso3   | geonombreFundar   | tipo_bien   |    expo |      prop |
#  |---:|-------:|:----------------|:------------------|:------------|--------:|----------:|
#  |  0 |   2023 | ALB             | Albania           | Primarios   | 7655.34 | 0.0200102 |
#  
#  ------------------------------
#  