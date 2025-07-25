from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')

    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio.isin([2007, 2023])'),
	rename_cols(map={'sector': 'categoria', 'prop_mujeres': 'valor'}),
	sort_values(how='ascending', by=['anio', 'categoria']),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='categoria', curr_value='Total economia', new_value='Total economía')
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
#  query(condition='anio.isin([2007, 2023])')
#  Index: 4 entries, 0 to 33
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          4 non-null      int64  
#   1   sector        4 non-null      object 
#   2   prop_mujeres  4 non-null      float64
#  
#  |    |   anio | sector   |   prop_mujeres |
#  |---:|-------:|:---------|---------------:|
#  |  0 |   2007 | SBC      |       0.384661 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'categoria', 'prop_mujeres': 'valor'})
#  Index: 4 entries, 0 to 33
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4 non-null      int64  
#   1   categoria  4 non-null      object 
#   2   valor      4 non-null      float64
#  
#  |    |   anio | categoria   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   2007 | SBC         | 0.384661 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'categoria'])
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4 non-null      int64  
#   1   categoria  4 non-null      object 
#   2   valor      4 non-null      float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2007 | SBC         | 38.4661 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4 non-null      int64  
#   1   categoria  4 non-null      object 
#   2   valor      4 non-null      float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2007 | SBC         | 38.4661 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Total economia', new_value='Total economía')
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4 non-null      int64  
#   1   categoria  4 non-null      object 
#   2   valor      4 non-null      float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   2007 | SBC         | 38.4661 |
#  
#  ------------------------------
#  