from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def drop_na(df, subset:str): 
    return df.dropna(subset=subset, axis=0)

@transformer.convert
def sort_mixed(
    df, 
    sort_instructions: dict
):
    """
    Sorts a DataFrame by multiple columns, supporting both categorical (with custom order) and numerical (with direction) sorting.

    Args:
        df: Input DataFrame.
        sort_instructions: Dictionary where keys are column names and values are either:
            - a list of categories (for categorical columns, sorted in that order)
            - a string 'ascending' or 'descending' (for numerical or string columns)

    Returns:
        DataFrame sorted by the specified columns in the given order/direction.
    """
    import pandas as pd

    by = []
    ascending = []

    for col, instruction in sort_instructions.items():
        if isinstance(instruction, list):
            # Categorical sort
            df[col] = pd.Categorical(df[col], categories=instruction, ordered=True)
            by.append(col)
            ascending.append(True)
        elif instruction in ['ascending', 'descending']:
            by.append(col)
            ascending.append(instruction == 'ascending')
        else:
            raise ValueError(f"Invalid sort instruction for column '{col}': {instruction}")

    return df.sort_values(by=by, ascending=ascending).reset_index(drop=True)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

@transformer.convert
def calcular_ranking(
    df: DataFrame, 
    index: list, 
    query = None,
    groupby = None, 
    rank_col: str = 'ranking', 
    value_col: str = 'valor', 
    method: str = 'dense',
    agg: str = 'sum'
):

    if query is not None and not isinstance(query, str):
        raise ValueError("query must be a string or None")

    # INSERT_YOUR_CODE
    if groupby is not None and not isinstance(groupby, list):
        raise ValueError("groupby must be a list or None")

    df_copy = df.copy()
    if query is not None:
        df_copy = df_copy.query(query)

    # If groupby is specified, aggregate value_col before ranking
    if groupby is not None:
        # Aggregate value_col
        df_copy = df_copy.groupby(groupby, as_index=False)[value_col].agg(agg)
        # Compute ranking
        df_copy[rank_col] = df_copy[value_col].rank(method=method, ascending=False)
        # Merge ranking back to original df on groupby columns
        df = df.merge(df_copy[groupby + [rank_col]], on=groupby, how='left')
    else:
        # Compute ranking without aggregation
        df_copy[rank_col] = df_copy[value_col].rank(method=method, ascending=False)
        df = df.merge(df_copy[index + [rank_col]], on=index, how='left')
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	latest_year(by='anio'),
	rename_cols(map={'particip_va_pib': 'valor', 'gran_sector': 'indicador'}),
	multiplicar_por_escalar(col='valor', k=100),
	calcular_ranking(index=['geonombreFundar'], query="tipo_sector == 'Servicios'", groupby=['geonombreFundar'], rank_col='ranking', value_col='valor', method='dense', agg='sum'),
	sort_mixed(sort_instructions={'ranking': 'descending', 'indicador': ['Agro y pesca', 'Industria manufacturera', 'Energía y minería', 'Construcción', 'Comercio, hotelería y restaurantes', 'Transporte y comunicaciones', 'Otros servicios']}),
	drop_na(subset='valor')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 81461 entries, 0 to 81460
#  Data columns (total 9 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  81461 non-null  object 
#   1   geonombreFundar  81461 non-null  object 
#   2   anio             81461 non-null  int64  
#   3   es_agregacion    81461 non-null  int64  
#   4   letra            81461 non-null  object 
#   5   gran_sector      81461 non-null  object 
#   6   id_tipo_sector   81461 non-null  int64  
#   7   tipo_sector      81461 non-null  object 
#   8   particip_va_pib  73145 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |---:|:------------------|:------------------|-------:|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  |  0 | AFG               | Afganistán        |   1970 |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.502422 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1537 non-null   object 
#   1   geonombreFundar  1537 non-null   object 
#   2   es_agregacion    1537 non-null   int64  
#   3   letra            1537 non-null   object 
#   4   gran_sector      1537 non-null   object 
#   5   id_tipo_sector   1537 non-null   int64  
#   6   tipo_sector      1537 non-null   object 
#   7   particip_va_pib  1439 non-null   float64
#  
#  |       | geocodigoFundar   | geonombreFundar   |   es_agregacion | letra   | gran_sector   |   id_tipo_sector | tipo_sector   |   particip_va_pib |
#  |------:|:------------------|:------------------|----------------:|:--------|:--------------|-----------------:|:--------------|------------------:|
#  | 79924 | AFG               | Afganistán        |               0 | AB      | Agro y pesca  |                1 | Bienes        |          0.355494 |
#  
#  ------------------------------
#  
#  rename_cols(map={'particip_va_pib': 'valor', 'gran_sector': 'indicador'})
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1537 non-null   object 
#   1   geonombreFundar  1537 non-null   object 
#   2   es_agregacion    1537 non-null   int64  
#   3   letra            1537 non-null   object 
#   4   indicador        1537 non-null   object 
#   5   id_tipo_sector   1537 non-null   int64  
#   6   tipo_sector      1537 non-null   object 
#   7   valor            1439 non-null   float64
#  
#  |       | geocodigoFundar   | geonombreFundar   |   es_agregacion | letra   | indicador    |   id_tipo_sector | tipo_sector   |   valor |
#  |------:|:------------------|:------------------|----------------:|:--------|:-------------|-----------------:|:--------------|--------:|
#  | 79924 | AFG               | Afganistán        |               0 | AB      | Agro y pesca |                1 | Bienes        | 35.5494 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  Index: 1537 entries, 79924 to 81460
#  Data columns (total 8 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  1537 non-null   object 
#   1   geonombreFundar  1537 non-null   object 
#   2   es_agregacion    1537 non-null   int64  
#   3   letra            1537 non-null   object 
#   4   indicador        1537 non-null   object 
#   5   id_tipo_sector   1537 non-null   int64  
#   6   tipo_sector      1537 non-null   object 
#   7   valor            1439 non-null   float64
#  
#  |       | geocodigoFundar   | geonombreFundar   |   es_agregacion | letra   | indicador    |   id_tipo_sector | tipo_sector   |   valor |
#  |------:|:------------------|:------------------|----------------:|:--------|:-------------|-----------------:|:--------------|--------:|
#  | 79924 | AFG               | Afganistán        |               0 | AB      | Agro y pesca |                1 | Bienes        | 35.5494 |
#  
#  ------------------------------
#  
#  calcular_ranking(index=['geonombreFundar'], query="tipo_sector == 'Servicios'", groupby=['geonombreFundar'], rank_col='ranking', value_col='valor', method='dense', agg='sum')
#  RangeIndex: 1537 entries, 0 to 1536
#  Data columns (total 9 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  1537 non-null   object  
#   1   geonombreFundar  1537 non-null   object  
#   2   es_agregacion    1537 non-null   int64   
#   3   letra            1537 non-null   object  
#   4   indicador        1537 non-null   category
#   5   id_tipo_sector   1537 non-null   int64   
#   6   tipo_sector      1537 non-null   object  
#   7   valor            1439 non-null   float64 
#   8   ranking          1537 non-null   float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   es_agregacion | letra   | indicador    |   id_tipo_sector | tipo_sector   |   valor |   ranking |
#  |---:|:------------------|:------------------|----------------:|:--------|:-------------|-----------------:|:--------------|--------:|----------:|
#  |  0 | AFG               | Afganistán        |               0 | AB      | Agro y pesca |                1 | Bienes        | 35.5494 |       161 |
#  
#  ------------------------------
#  
#  sort_mixed(sort_instructions={'ranking': 'descending', 'indicador': ['Agro y pesca', 'Industria manufacturera', 'Energía y minería', 'Construcción', 'Comercio, hotelería y restaurantes', 'Transporte y comunicaciones', 'Otros servicios']})
#  RangeIndex: 1537 entries, 0 to 1536
#  Data columns (total 9 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  1537 non-null   object  
#   1   geonombreFundar  1537 non-null   object  
#   2   es_agregacion    1537 non-null   int64   
#   3   letra            1537 non-null   object  
#   4   indicador        1537 non-null   category
#   5   id_tipo_sector   1537 non-null   int64   
#   6   tipo_sector      1537 non-null   object  
#   7   valor            1439 non-null   float64 
#   8   ranking          1537 non-null   float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   es_agregacion | letra   | indicador    |   id_tipo_sector | tipo_sector   |   valor |   ranking |
#  |---:|:------------------|:------------------|----------------:|:--------|:-------------|-----------------:|:--------------|--------:|----------:|
#  |  0 | CPV               | Cabo Verde        |               0 | AB      | Agro y pesca |                1 | Bienes        |     nan |       207 |
#  
#  ------------------------------
#  
#  drop_na(subset='valor')
#  Index: 1439 entries, 84 to 1536
#  Data columns (total 9 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   geocodigoFundar  1439 non-null   object  
#   1   geonombreFundar  1439 non-null   object  
#   2   es_agregacion    1439 non-null   int64   
#   3   letra            1439 non-null   object  
#   4   indicador        1439 non-null   category
#   5   id_tipo_sector   1439 non-null   int64   
#   6   tipo_sector      1439 non-null   object  
#   7   valor            1439 non-null   float64 
#   8   ranking          1439 non-null   float64 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   es_agregacion | letra   | indicador    |   id_tipo_sector | tipo_sector   |   valor |   ranking |
#  |---:|:------------------|:------------------|----------------:|:--------|:-------------|-----------------:|:--------------|--------:|----------:|
#  | 84 | LBR               | Liberia           |               0 | AB      | Agro y pesca |                1 | Bienes        | 73.9551 |       206 |
#  
#  ------------------------------
#  