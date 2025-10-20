from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def filtrar_ultimo_anio(df:DataFrame, group_cols:list[str], anio_col:str='anio'):
    """
    Filtra el último año disponible dentro de cada grupo.

    Parámetros:
        df (pd.DataFrame): DataFrame de entrada.
        group_cols (list o str): Columnas que definen los grupos.
        anio_col (str): Nombre de la columna de año. Por defecto 'anio'.

    Retorna:
        pd.DataFrame: DataFrame filtrado con solo el último año por grupo.
    """
    if isinstance(group_cols, str):
        group_cols = [group_cols]

    # Obtener el índice del máximo año por grupo
    idx = df.groupby(group_cols)[anio_col].idxmax()

    # Filtrar usando esos índices
    return df.loc[idx].reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	filtrar_ultimo_anio(group_cols=['geonombreFundar'], anio_col='anio')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 576 entries, 0 to 575
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             576 non-null    int64  
#   1   geocodigoFundar  576 non-null    object 
#   2   geonombreFundar  576 non-null    object 
#   3   share_indust_id  576 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   share_indust_id |
#  |---:|-------:|:------------------|:------------------|------------------:|
#  |  0 |   2011 | ISL               | Islandia          |           44.0774 |
#  
#  ------------------------------
#  
#  filtrar_ultimo_anio(group_cols=['geonombreFundar'], anio_col='anio')
#  RangeIndex: 44 entries, 0 to 43
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             44 non-null     int64  
#   1   geocodigoFundar  44 non-null     object 
#   2   geonombreFundar  44 non-null     object 
#   3   share_indust_id  44 non-null     float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   share_indust_id |
#  |---:|-------:|:------------------|:------------------|------------------:|
#  |  0 |   2021 | DEU               | Alemania          |           82.6784 |
#  
#  ------------------------------
#  