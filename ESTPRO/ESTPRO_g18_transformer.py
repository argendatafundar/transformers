from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='anio == 2019'),
	query(condition='geocodigoFundar.isin(["OECD"])'),
	rename_cols(map={'letra_desc_abrev': 'categoria', 'valor_relativo_arg': 'valor', 'geocodigoFundar': 'geocodigo'}),
	drop_col(col='es_agregacion', axis=1),
	drop_col(col='anio', axis=1),
	drop_col(col='letra', axis=1),
	sort_mixed(sort_instructions={'valor': 'ascending'})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 36399 entries, 0 to 36398
#  Data columns (total 7 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     36399 non-null  object 
#   1   geonombreFundar     36399 non-null  object 
#   2   anio                36399 non-null  int64  
#   3   letra               36399 non-null  object 
#   4   es_agregacion       36399 non-null  int64  
#   5   letra_desc_abrev    36399 non-null  object 
#   6   valor_relativo_arg  36399 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | letra   |   es_agregacion | letra_desc_abrev   |   valor_relativo_arg |
#  |---:|:------------------|:------------------|-------:|:--------|----------------:|:-------------------|---------------------:|
#  |  0 | ARG               | Argentina         |   1995 | A       |               0 | Agro y pesca       |                  100 |
#  
#  ------------------------------
#  
#  query(condition='anio == 2019')
#  Index: 1400 entries, 33599 to 34998
#  Data columns (total 7 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     1400 non-null   object 
#   1   geonombreFundar     1400 non-null   object 
#   2   anio                1400 non-null   int64  
#   3   letra               1400 non-null   object 
#   4   es_agregacion       1400 non-null   int64  
#   5   letra_desc_abrev    1400 non-null   object 
#   6   valor_relativo_arg  1400 non-null   float64
#  
#  |       | geocodigoFundar   | geonombreFundar   |   anio | letra   |   es_agregacion | letra_desc_abrev   |   valor_relativo_arg |
#  |------:|:------------------|:------------------|-------:|:--------|----------------:|:-------------------|---------------------:|
#  | 33599 | ARG               | Argentina         |   2019 | A       |               0 | Agro y pesca       |                  100 |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar.isin(["OECD"])')
#  Index: 20 entries, 33648 to 34978
#  Data columns (total 7 columns):
#   #   Column              Non-Null Count  Dtype  
#  ---  ------              --------------  -----  
#   0   geocodigoFundar     20 non-null     object 
#   1   geonombreFundar     20 non-null     object 
#   2   anio                20 non-null     int64  
#   3   letra               20 non-null     object 
#   4   es_agregacion       20 non-null     int64  
#   5   letra_desc_abrev    20 non-null     object 
#   6   valor_relativo_arg  20 non-null     float64
#  
#  |       | geocodigoFundar   | geonombreFundar         |   anio | letra   |   es_agregacion | letra_desc_abrev   |   valor_relativo_arg |
#  |------:|:------------------|:------------------------|-------:|:--------|----------------:|:-------------------|---------------------:|
#  | 33648 | OECD              | Países miembros de OCDE |   2019 | A       |               1 | Agro y pesca       |              174.053 |
#  
#  ------------------------------
#  
#  rename_cols(map={'letra_desc_abrev': 'categoria', 'valor_relativo_arg': 'valor', 'geocodigoFundar': 'geocodigo'})
#  Index: 20 entries, 33648 to 34978
#  Data columns (total 7 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        20 non-null     object 
#   1   geonombreFundar  20 non-null     object 
#   2   anio             20 non-null     int64  
#   3   letra            20 non-null     object 
#   4   es_agregacion    20 non-null     int64  
#   5   categoria        20 non-null     object 
#   6   valor            20 non-null     float64
#  
#  |       | geocodigo   | geonombreFundar         |   anio | letra   |   es_agregacion | categoria    |   valor |
#  |------:|:------------|:------------------------|-------:|:--------|----------------:|:-------------|--------:|
#  | 33648 | OECD        | Países miembros de OCDE |   2019 | A       |               1 | Agro y pesca | 174.053 |
#  
#  ------------------------------
#  
#  drop_col(col='es_agregacion', axis=1)
#  Index: 20 entries, 33648 to 34978
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        20 non-null     object 
#   1   geonombreFundar  20 non-null     object 
#   2   anio             20 non-null     int64  
#   3   letra            20 non-null     object 
#   4   categoria        20 non-null     object 
#   5   valor            20 non-null     float64
#  
#  |       | geocodigo   | geonombreFundar         |   anio | letra   | categoria    |   valor |
#  |------:|:------------|:------------------------|-------:|:--------|:-------------|--------:|
#  | 33648 | OECD        | Países miembros de OCDE |   2019 | A       | Agro y pesca | 174.053 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 20 entries, 33648 to 34978
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        20 non-null     object 
#   1   geonombreFundar  20 non-null     object 
#   2   letra            20 non-null     object 
#   3   categoria        20 non-null     object 
#   4   valor            20 non-null     float64
#  
#  |       | geocodigo   | geonombreFundar         | letra   | categoria    |   valor |
#  |------:|:------------|:------------------------|:--------|:-------------|--------:|
#  | 33648 | OECD        | Países miembros de OCDE | A       | Agro y pesca | 174.053 |
#  
#  ------------------------------
#  
#  drop_col(col='letra', axis=1)
#  Index: 20 entries, 33648 to 34978
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        20 non-null     object 
#   1   geonombreFundar  20 non-null     object 
#   2   categoria        20 non-null     object 
#   3   valor            20 non-null     float64
#  
#  |       | geocodigo   | geonombreFundar         | categoria    |   valor |
#  |------:|:------------|:------------------------|:-------------|--------:|
#  | 33648 | OECD        | Países miembros de OCDE | Agro y pesca | 174.053 |
#  
#  ------------------------------
#  
#  sort_mixed(sort_instructions={'valor': 'ascending'})
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        20 non-null     object 
#   1   geonombreFundar  20 non-null     object 
#   2   categoria        20 non-null     object 
#   3   valor            20 non-null     float64
#  
#  |    | geocodigo   | geonombreFundar         | categoria          |   valor |
#  |---:|:------------|:------------------------|:-------------------|--------:|
#  |  0 | OECD        | Países miembros de OCDE | Petróleo y minería | 164.809 |
#  
#  ------------------------------
#  