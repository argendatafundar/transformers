from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def drop_cols(df, cols):
    return df.drop(cols)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)

@transformer.convert
def calcular_porcentaje_por_grupo(df, group_cols: list, valor_col: str = 'valor'):
    """
    Agrupa por una lista de columnas y año, y recalcula la columna valor como porcentaje por grupo.

    Args:
        df: DataFrame de pandas
        group_cols: Lista de columnas para agrupar (además de anio)
        valor_col: Nombre de la columna de valores a convertir a porcentaje (default: 'valor')
        anio_col: Nombre de la columna de año (default: 'anio')

    Returns:
        DataFrame con la columna valor convertida a porcentaje por grupo y año
    """
    df_ = df.copy()
    groupby_cols = group_cols

    # Calcular el total por grupo (año + otras columnas)
    df_['_total_grupo'] = df_.groupby(groupby_cols)[valor_col].transform('sum')

    # Calcular el porcentaje
    df_[valor_col] = (df_[valor_col] / df_['_total_grupo']) * 100

    # Eliminar columna temporal
    df_ = df_.drop('_total_grupo', axis=1)

    return df_

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_cols(cols=['unit']),
	to_pandas(dummy=True),
	rename_cols(map={'sector': 'indicador', 'valor_en_ggco2e': 'valor'}),
	calcular_porcentaje_por_grupo(group_cols=['anio', 'geocodigoFundar', 'entity', 'geonombreFundar'], valor_col='valor'),
	sort_values_by_comparison(colname='indicador', precedence={'Energía': 0, 'AGSyOUT': 1, 'PIUP': 2, 'Residuos': 3, 'Otros': 4}, prefix=['anio'], suffix=[])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  drop_cols(cols=['unit'])
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 1375 entries, 0 to 1374
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             1375 non-null   int64  
#   1   sector           1375 non-null   object 
#   2   geocodigoFundar  1375 non-null   object 
#   3   entity           1375 non-null   object 
#   4   valor_en_ggco2e  1375 non-null   float64
#   5   geonombreFundar  1375 non-null   object 
#  
#  |    |   anio | sector   | geocodigoFundar   | entity               |   valor_en_ggco2e | geonombreFundar   |
#  |---:|-------:|:---------|:------------------|:---------------------|------------------:|:------------------|
#  |  0 |   1750 | AGSyOUT  | WLD               | KYOTOGHG (AR6GWP100) |            558000 | Mundo             |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador', 'valor_en_ggco2e': 'valor'})
#  RangeIndex: 1375 entries, 0 to 1374
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             1375 non-null   int64  
#   1   indicador        1375 non-null   object 
#   2   geocodigoFundar  1375 non-null   object 
#   3   entity           1375 non-null   object 
#   4   valor            1375 non-null   float64
#   5   geonombreFundar  1375 non-null   object 
#  
#  |    |   anio | indicador   | geocodigoFundar   | entity               |   valor | geonombreFundar   |
#  |---:|-------:|:------------|:------------------|:---------------------|--------:|:------------------|
#  |  0 |   1750 | AGSyOUT     | WLD               | KYOTOGHG (AR6GWP100) |  558000 | Mundo             |
#  
#  ------------------------------
#  
#  calcular_porcentaje_por_grupo(group_cols=['anio', 'geocodigoFundar', 'entity', 'geonombreFundar'], valor_col='valor')
#  RangeIndex: 1375 entries, 0 to 1374
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             1375 non-null   int64  
#   1   indicador        1375 non-null   object 
#   2   geocodigoFundar  1375 non-null   object 
#   3   entity           1375 non-null   object 
#   4   valor            1375 non-null   float64
#   5   geonombreFundar  1375 non-null   object 
#  
#  |    |   anio | indicador   | geocodigoFundar   | entity               |   valor | geonombreFundar   |
#  |---:|-------:|:------------|:------------------|:---------------------|--------:|:------------------|
#  |  0 |   1750 | AGSyOUT     | WLD               | KYOTOGHG (AR6GWP100) | 70.1972 | Mundo             |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Energía': 0, 'AGSyOUT': 1, 'PIUP': 2, 'Residuos': 3, 'Otros': 4}, prefix=['anio'], suffix=[])
#  Index: 1375 entries, 1 to 1372
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             1375 non-null   int64  
#   1   indicador        1375 non-null   object 
#   2   geocodigoFundar  1375 non-null   object 
#   3   entity           1375 non-null   object 
#   4   valor            1375 non-null   float64
#   5   geonombreFundar  1375 non-null   object 
#  
#  |    |   anio | indicador   | geocodigoFundar   | entity               |   valor | geonombreFundar   |
#  |---:|-------:|:------------|:------------------|:---------------------|--------:|:------------------|
#  |  1 |   1750 | Energía     | WLD               | KYOTOGHG (AR6GWP100) | 19.1218 | Mundo             |
#  
#  ------------------------------
#  