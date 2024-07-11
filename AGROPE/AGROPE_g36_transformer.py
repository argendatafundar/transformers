from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tipo_cadena': 'categoria'}),
	sort_values(how='ascending', by=['categoria']),
	drop_na(col='valor'),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   tipo_cadena  32 non-null     object 
#   1   valor        32 non-null     float64
#  
#  |    | tipo_cadena   |   valor |
#  |---:|:--------------|--------:|
#  |  0 | Miel          |    0.86 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_cadena': 'categoria'})
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   valor      32 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Miel        |    0.86 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['categoria'])
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   valor      32 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Ajo         |    0.11 |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   valor      32 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Ajo         |      11 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  32 non-null     object 
#   1   valor      32 non-null     float64
#  
#  |    | categoria   |   valor |
#  |---:|:------------|--------:|
#  |  0 | Ajo         |      11 |
#  
#  ------------------------------
#  