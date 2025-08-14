from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, mapping: dict):
    df = df.replace(mapping)
    return df

@transformer.convert
def summarize(df: DataFrame, groupby: list, columns: list = None, operation: str = 'sum'):
    """
    Agrupa el DataFrame por las columnas indicadas y aplica la operación de agregación especificada
    sobre las columnas seleccionadas.

    Args:
        df: DataFrame de entrada (pandas).
        groupby: Lista de columnas para agrupar.
        columns: Lista de columnas sobre las que aplicar la operación. Si es None, se aplica a todas las columnas numéricas.
        operation: Operación de agregación como string (por ejemplo, 'sum', 'mean', etc.).

    Returns:
        DataFrame con los resultados agregados, no agrupado.
    """
    if columns is not None:
        grouped = df.groupby(groupby)[columns].agg(operation)
    else:
        grouped = df.groupby(groupby).agg(operation)
    # Si el resultado tiene un índice múltiple, lo reseteamos
    result = grouped.reset_index()
    return result

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='geocodigoFundar == "ARG"'),
	drop_col(col='geocodigoFundar', axis=1),
	drop_col(col='location_name_short_en', axis=1),
	drop_col(col='sitc_2_1_cod', axis=1),
	rename_cols(map={'year': 'anio', 'sitc_product_name_es': 'indicador', 'export_value_pc': 'valor'}),
	replace_value(mapping={'indicador': {'Maquinaria y material de transporte': 'Maquinaria y mat. de transp.', 'Productos químicos': 'Prod. químicos', 'Combustibles minerales, lubricantes y productos similares': 'Comb. minerales y lubricantes', 'Materiales crudos, no comestibles, excepto combustibles': 'Materiales crudos', 'Aceites y mantecas de origen animal y vegetal': 'Aceites y mantecas', 'Bebidas y tabaco': 'Bebidas y tabaco', 'Productos alimenticios': 'Prod. alimenticios', 'Artículos manufacturados, clasificados principalmente según el material': 'Artículos manufact', 'Artículos manufacturados diversos': 'Artículos manufact', 'Transacciones y mercaderías diversas, N. E. P.': 'Otros no clasificados'}}),
	summarize(groupby=['geonombreFundar', 'anio', 'indicador'], columns='valor', operation='sum')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 122560 entries, 0 to 122559
#  Data columns (total 7 columns):
#   #   Column                  Non-Null Count   Dtype  
#  ---  ------                  --------------   -----  
#   0   geocodigoFundar         122560 non-null  object 
#   1   geonombreFundar         122560 non-null  object 
#   2   year                    122560 non-null  int64  
#   3   location_name_short_en  122560 non-null  object 
#   4   sitc_2_1_cod            122560 non-null  int64  
#   5   sitc_product_name_es    122560 non-null  object 
#   6   export_value_pc         122560 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | location_name_short_en   |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|:-------------------------|---------------:|:-----------------------|------------------:|
#  |  0 | AFG               | Afganistán        |   1962 | Afghanistan              |              0 | Productos alimenticios |           22.4199 |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar == "ARG"')
#  Index: 600 entries, 50 to 120309
#  Data columns (total 7 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         600 non-null    object 
#   1   geonombreFundar         600 non-null    object 
#   2   year                    600 non-null    int64  
#   3   location_name_short_en  600 non-null    object 
#   4   sitc_2_1_cod            600 non-null    int64  
#   5   sitc_product_name_es    600 non-null    object 
#   6   export_value_pc         600 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   year | location_name_short_en   |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|:-------------------------|---------------:|:-----------------------|------------------:|
#  | 50 | ARG               | Argentina         |   1962 | Argentina                |              0 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 600 entries, 50 to 120309
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geonombreFundar         600 non-null    object 
#   1   year                    600 non-null    int64  
#   2   location_name_short_en  600 non-null    object 
#   3   sitc_2_1_cod            600 non-null    int64  
#   4   sitc_product_name_es    600 non-null    object 
#   5   export_value_pc         600 non-null    float64
#  
#  |    | geonombreFundar   |   year | location_name_short_en   |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|:------------------|-------:|:-------------------------|---------------:|:-----------------------|------------------:|
#  | 50 | Argentina         |   1962 | Argentina                |              0 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  drop_col(col='location_name_short_en', axis=1)
#  Index: 600 entries, 50 to 120309
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geonombreFundar       600 non-null    object 
#   1   year                  600 non-null    int64  
#   2   sitc_2_1_cod          600 non-null    int64  
#   3   sitc_product_name_es  600 non-null    object 
#   4   export_value_pc       600 non-null    float64
#  
#  |    | geonombreFundar   |   year |   sitc_2_1_cod | sitc_product_name_es   |   export_value_pc |
#  |---:|:------------------|-------:|---------------:|:-----------------------|------------------:|
#  | 50 | Argentina         |   1962 |              0 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  drop_col(col='sitc_2_1_cod', axis=1)
#  Index: 600 entries, 50 to 120309
#  Data columns (total 4 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geonombreFundar       600 non-null    object 
#   1   year                  600 non-null    int64  
#   2   sitc_product_name_es  600 non-null    object 
#   3   export_value_pc       600 non-null    float64
#  
#  |    | geonombreFundar   |   year | sitc_product_name_es   |   export_value_pc |
#  |---:|:------------------|-------:|:-----------------------|------------------:|
#  | 50 | Argentina         |   1962 | Productos alimenticios |           65.8647 |
#  
#  ------------------------------
#  
#  rename_cols(map={'year': 'anio', 'sitc_product_name_es': 'indicador', 'export_value_pc': 'valor'})
#  Index: 600 entries, 50 to 120309
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  600 non-null    object 
#   1   anio             600 non-null    int64  
#   2   indicador        600 non-null    object 
#   3   valor            600 non-null    float64
#  
#  |    | geonombreFundar   |   anio | indicador              |   valor |
#  |---:|:------------------|-------:|:-----------------------|--------:|
#  | 50 | Argentina         |   1962 | Productos alimenticios | 65.8647 |
#  
#  ------------------------------
#  
#  replace_value(mapping={'indicador': {'Maquinaria y material de transporte': 'Maquinaria y mat. de transp.', 'Productos químicos': 'Prod. químicos', 'Combustibles minerales, lubricantes y productos similares': 'Comb. minerales y lubricantes', 'Materiales crudos, no comestibles, excepto combustibles': 'Materiales crudos', 'Aceites y mantecas de origen animal y vegetal': 'Aceites y mantecas', 'Bebidas y tabaco': 'Bebidas y tabaco', 'Productos alimenticios': 'Prod. alimenticios', 'Artículos manufacturados, clasificados principalmente según el material': 'Artículos manufact', 'Artículos manufacturados diversos': 'Artículos manufact', 'Transacciones y mercaderías diversas, N. E. P.': 'Otros no clasificados'}})
#  Index: 600 entries, 50 to 120309
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  600 non-null    object 
#   1   anio             600 non-null    int64  
#   2   indicador        600 non-null    object 
#   3   valor            600 non-null    float64
#  
#  |    | geonombreFundar   |   anio | indicador          |   valor |
#  |---:|:------------------|-------:|:-------------------|--------:|
#  | 50 | Argentina         |   1962 | Prod. alimenticios | 65.8647 |
#  
#  ------------------------------
#  
#  summarize(groupby=['geonombreFundar', 'anio', 'indicador'], columns='valor', operation='sum')
#  RangeIndex: 540 entries, 0 to 539
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geonombreFundar  540 non-null    object 
#   1   anio             540 non-null    int64  
#   2   indicador        540 non-null    object 
#   3   valor            540 non-null    float64
#  
#  |    | geonombreFundar   |   anio | indicador          |   valor |
#  |---:|:------------------|-------:|:-------------------|--------:|
#  |  0 | Argentina         |   1962 | Aceites y mantecas | 5.89688 |
#  
#  ------------------------------
#  