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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'prop_expo': 'valor'}),
	replace_value(col='geocodigo', curr_value='ROM', new_value='ROU'),
	replace_value(col='geocodigo', curr_value='CHT', new_value='TWN')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 3194 entries, 0 to 3193
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   iso3       3194 non-null   object 
#   1   anio       3194 non-null   int64  
#   2   prop_expo  3194 non-null   float64
#  
#  |    | iso3   |   anio |   prop_expo |
#  |---:|:-------|-------:|------------:|
#  |  0 | AFG    |   2008 | 0.000248476 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'prop_expo': 'valor'})
#  RangeIndex: 3194 entries, 0 to 3193
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  3194 non-null   object 
#   1   anio       3194 non-null   int64  
#   2   valor      3194 non-null   float64
#  
#  |    | geocodigo   |   anio |       valor |
#  |---:|:------------|-------:|------------:|
#  |  0 | AFG         |   2008 | 0.000248476 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ROM', new_value='ROU')
#  RangeIndex: 3194 entries, 0 to 3193
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  3194 non-null   object 
#   1   anio       3194 non-null   int64  
#   2   valor      3194 non-null   float64
#  
#  |    | geocodigo   |   anio |       valor |
#  |---:|:------------|-------:|------------:|
#  |  0 | AFG         |   2008 | 0.000248476 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='CHT', new_value='TWN')
#  RangeIndex: 3194 entries, 0 to 3193
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  3194 non-null   object 
#   1   anio       3194 non-null   int64  
#   2   valor      3194 non-null   float64
#  
#  |    | geocodigo   |   anio |       valor |
#  |---:|:------------|-------:|------------:|
#  |  0 | AFG         |   2008 | 0.000248476 |
#  
#  ------------------------------
#  