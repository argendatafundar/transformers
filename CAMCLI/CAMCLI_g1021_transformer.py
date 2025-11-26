from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: pl.DataFrame, dummy: bool = True):
    """Convert a Polars DataFrame to a pandas DataFrame."""
    return df.to_pandas()

@transformer.convert
def agrupar_y_sumar(df: DataFrame, col_indicador, col_anio):
    """Group by specified columns and sum the results."""
    return df.groupby([col_indicador, col_anio]).sum().reset_index()

@transformer.convert
def recategorizar_resto(df, col,  seleccion, etiqueta):
    df = df.copy()
    df.loc[~df[col].isin(seleccion), col] = etiqueta
    return df

@transformer.convert
def drop_cols(df, cols):
    return df.drop(cols)

@transformer.convert
def filter_equals(df, col, valor, negacion = False):
    df = df.copy()

    if negacion:
        df = df[df[col] != valor]
    else:
        df = df[df[col] == valor]    

    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col: str, k: float):
    """Multiply a given column by a scalar k."""
    df[col] = df[col] * k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	drop_cols(cols='geocodigoFundar'),
	to_pandas(dummy=True),
	filter_equals(col='geonombreFundar', valor='Mundo', negacion=True),
	recategorizar_resto(col='geonombreFundar', seleccion=['Estados Unidos', 'China', 'Rusia', 'India', 'Brasil', 'Alemania', 'Indonesia', 'Reino Unido', 'Japón', 'Canadá', 'Argentina'], etiqueta='Resto'),
	agrupar_y_sumar(col_indicador='geonombreFundar', col_anio='anio'),
	multiplicar_por_escalar(col='valor', k=1e-06)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  drop_cols(cols='geocodigoFundar')
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 34278 entries, 0 to 34277
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  34278 non-null  object 
#   1   anio             34278 non-null  int64  
#   2   valor            34278 non-null  float64
#  
#  |    | geonombreFundar   |   anio |       valor |
#  |---:|:------------------|-------:|------------:|
#  |  0 | Afganistán        |   1850 | 7.43574e+06 |
#  
#  ------------------------------
#  
#  filter_equals(col='geonombreFundar', valor='Mundo', negacion=True)
#  Index: 34104 entries, 0 to 34277
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  34104 non-null  object 
#   1   anio             34104 non-null  int64  
#   2   valor            34104 non-null  float64
#  
#  |    | geonombreFundar   |   anio |       valor |
#  |---:|:------------------|-------:|------------:|
#  |  0 | Afganistán        |   1850 | 7.43574e+06 |
#  
#  ------------------------------
#  
#  recategorizar_resto(col='geonombreFundar', seleccion=['Estados Unidos', 'China', 'Rusia', 'India', 'Brasil', 'Alemania', 'Indonesia', 'Reino Unido', 'Japón', 'Canadá', 'Argentina'], etiqueta='Resto')
#  Index: 34104 entries, 0 to 34277
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  34104 non-null  object 
#   1   anio             34104 non-null  int64  
#   2   valor            34104 non-null  float64
#  
#  |    | geonombreFundar   |   anio |       valor |
#  |---:|:------------------|-------:|------------:|
#  |  0 | Resto             |   1850 | 7.43574e+06 |
#  
#  ------------------------------
#  
#  agrupar_y_sumar(col_indicador='geonombreFundar', col_anio='anio')
#  RangeIndex: 2088 entries, 0 to 2087
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  2088 non-null   object 
#   1   anio             2088 non-null   int64  
#   2   valor            2088 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|-------:|--------:|
#  |  0 | Alemania          |   1850 | 78.8999 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  RangeIndex: 2088 entries, 0 to 2087
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  2088 non-null   object 
#   1   anio             2088 non-null   int64  
#   2   valor            2088 non-null   float64
#  
#  |    | geonombreFundar   |   anio |   valor |
#  |---:|:------------------|-------:|--------:|
#  |  0 | Alemania          |   1850 | 78.8999 |
#  
#  ------------------------------
#  