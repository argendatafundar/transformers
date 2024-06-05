from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='ROM', new_value='ROU'),
	rename_cols(map={'iso3': 'geocodigo'}),
	rename_cols(map={'prop_expo': 'valor'})
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
#  replace_value(col='iso3', curr_value='ROM', new_value='ROU')
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
#  rename_cols(map={'iso3': 'geocodigo'})
#  RangeIndex: 2198 entries, 0 to 2197
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  2198 non-null   object 
#   1   anio       2198 non-null   int64  
#   2   prop_expo  2198 non-null   float64
#  
#  |    | geocodigo   |   anio |   prop_expo |
#  |---:|:------------|-------:|------------:|
#  |  0 | AFG         |   2008 |  9.6217e-05 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop_expo': 'valor'})
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