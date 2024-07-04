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
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'prop_expo': 'valor'}),
	replace_value(col='geocodigo', curr_value='ROM', new_value='ROU'),
	sort_values(how='ascending', by=['anio', 'geocodigo']),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 2198 entries, 0 to 2197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       2198 non-null   object 
#   1   anio       2198 non-null   int64  
#   2   prop_expo  2198 non-null   float64
#  
#  |    | iso3   |   anio |   prop_expo |
#  |---:|:-------|-------:|------------:|
#  |  0 | AFG    |   2008 |  9.6217e-05 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'prop_expo': 'valor'})
#  RangeIndex: 2198 entries, 0 to 2197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  2198 non-null   object 
#   1   anio       2198 non-null   int64  
#   2   valor      2198 non-null   float64
#  
#  |    | geocodigo   |   anio |      valor |
#  |---:|:------------|-------:|-----------:|
#  |  0 | AFG         |   2008 | 9.6217e-05 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ROM', new_value='ROU')
#  RangeIndex: 2198 entries, 0 to 2197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  2198 non-null   object 
#   1   anio       2198 non-null   int64  
#   2   valor      2198 non-null   float64
#  
#  |    | geocodigo   |   anio |      valor |
#  |---:|:------------|-------:|-----------:|
#  |  0 | AFG         |   2008 | 9.6217e-05 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'geocodigo'])
#  RangeIndex: 2198 entries, 0 to 2197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  2198 non-null   object 
#   1   anio       2198 non-null   int64  
#   2   valor      2198 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AGO         |   2005 |       0 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 2198 entries, 0 to 2197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  2198 non-null   object 
#   1   anio       2198 non-null   int64  
#   2   valor      2198 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AGO         |   2005 |       0 |
#  
#  ------------------------------
#  