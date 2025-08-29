from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def quedarse_con_n_anios(df: DataFrame, year_col:str, n_years:int):
    years = df[year_col].sort_values().unique().tolist()
    L = len(years) - 1  # último índice
    idx = [round(i * L / (n - 1)) for i in range(n_years)]
    filter_years = [years[i] for i in idx]
    return  df[df[year_col].isin(filter_years)]

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	quedarse_con_n_anios(year_col='anio', n_years=4),
	rename_cols(map={'sector': 'categoria', 'prop_mujeres': 'valor'}),
	sort_values(how='ascending', by=['anio', 'categoria']),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='categoria', curr_value='Total economia', new_value='Total economía', mapping=None)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 34 entries, 0 to 33
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          34 non-null     int64  
#   1   sector        34 non-null     object 
#   2   prop_mujeres  34 non-null     float64
#  
#  |    |   anio | sector   |   prop_mujeres |
#  |---:|-------:|:---------|---------------:|
#  |  0 |   2007 | SBC      |       0.384661 |
#  
#  ------------------------------
#  
#  quedarse_con_n_anios(year_col='anio', n_years=4)
#  Index: 8 entries, 0 to 9
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          8 non-null      int64  
#   1   sector        8 non-null      object 
#   2   prop_mujeres  8 non-null      float64
#  
#  |    |   anio | sector   |   prop_mujeres |
#  |---:|-------:|:---------|---------------:|
#  |  0 |   2007 | SBC      |       0.384661 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'categoria', 'prop_mujeres': 'valor'})
#  Index: 8 entries, 0 to 9
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       8 non-null      int64  
#   1   categoria  8 non-null      object 
#   2   valor      8 non-null      float64
#  
#  |    |   anio | categoria   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   2007 | SBC         | 0.384661 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'categoria'])
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       8 non-null      int64  
#   1   categoria  8 non-null      object 
#   2   valor      8 non-null      float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2007 | SBC         | 38.4661 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       8 non-null      int64  
#   1   categoria  8 non-null      object 
#   2   valor      8 non-null      float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2007 | SBC         | 38.4661 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Total economia', new_value='Total economía', mapping=None)
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       8 non-null      int64  
#   1   categoria  8 non-null      object 
#   2   valor      8 non-null      float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2007 | SBC         | 38.4661 |
#  
#  ------------------------------
#  