from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'valor_en_ton': 'valor'}),
	replace_value(col='geocodigo', curr_value='OWID_KOS', new_value='XKX'),
	replace_value(col='geocodigo', curr_value='OWID_WRL', new_value='WLD'),
	drop_na(col='valor'),
	sort_values(how='ascending', by=['anio', 'geocodigo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 23046 entries, 0 to 23045
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   iso3          23046 non-null  object 
#   1   anio          23046 non-null  int64  
#   2   valor_en_ton  23046 non-null  float64
#  
#  |    | iso3   |   anio |   valor_en_ton |
#  |---:|:-------|-------:|---------------:|
#  |  0 | AFG    |   1949 |     0.00199215 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'valor_en_ton': 'valor'})
#  RangeIndex: 23046 entries, 0 to 23045
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  23046 non-null  object 
#   1   anio       23046 non-null  int64  
#   2   valor      23046 non-null  float64
#  
#  |    | geocodigo   |   anio |      valor |
#  |---:|:------------|-------:|-----------:|
#  |  0 | AFG         |   1949 | 0.00199215 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='OWID_KOS', new_value='XKX')
#  RangeIndex: 23046 entries, 0 to 23045
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  23046 non-null  object 
#   1   anio       23046 non-null  int64  
#   2   valor      23046 non-null  float64
#  
#  |    | geocodigo   |   anio |      valor |
#  |---:|:------------|-------:|-----------:|
#  |  0 | AFG         |   1949 | 0.00199215 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 23046 entries, 0 to 23045
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  23046 non-null  object 
#   1   anio       23046 non-null  int64  
#   2   valor      23046 non-null  float64
#  
#  |    | geocodigo   |   anio |      valor |
#  |---:|:------------|-------:|-----------:|
#  |  0 | AFG         |   1949 | 0.00199215 |
#  
#  ------------------------------
#  
#  drop_na(col='valor')
#  RangeIndex: 23046 entries, 0 to 23045
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  23046 non-null  object 
#   1   anio       23046 non-null  int64  
#   2   valor      23046 non-null  float64
#  
#  |    | geocodigo   |   anio |      valor |
#  |---:|:------------|-------:|-----------:|
#  |  0 | AFG         |   1949 | 0.00199215 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 23046 entries, 0 to 23045
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  23046 non-null  object 
#   1   anio       23046 non-null  int64  
#   2   valor      23046 non-null  float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AND         |   1750 |       0 |
#  
#  ------------------------------
#  