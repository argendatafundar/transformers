from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    return df.sort_values(by=by, ascending=how == 'ascending')
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'vab': 'valor', 'anio': 'categoria'}),
	mutiplicar_por_escalar(col='valor', k=100),
	sort_values(how='ascending', by=['categoria'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    4 non-null      int64  
#   1   vab     4 non-null      float64
#  
#  |    |   anio |    vab |
#  |---:|-------:|-------:|
#  |  0 |   2004 | 0.0046 |
#  
#  ------------------------------
#  
#  rename_cols(map={'vab': 'valor', 'anio': 'categoria'})
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  4 non-null      int64  
#   1   valor      4 non-null      float64
#  
#  |    |   categoria |   valor |
#  |---:|------------:|--------:|
#  |  0 |        2004 |    0.46 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  4 non-null      int64  
#   1   valor      4 non-null      float64
#  
#  |    |   categoria |   valor |
#  |---:|------------:|--------:|
#  |  0 |        2004 |    0.46 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['categoria'])
#  RangeIndex: 4 entries, 0 to 3
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  4 non-null      int64  
#   1   valor      4 non-null      float64
#  
#  |    |   categoria |   valor |
#  |---:|------------:|--------:|
#  |  0 |        2004 |    0.46 |
#  
#  ------------------------------
#  