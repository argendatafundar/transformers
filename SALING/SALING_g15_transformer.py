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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'    ano': 'anio', 'variable': 'indicador'}),
	replace_value(col='indicador', curr_value='AmericaLatina', new_value='América Latina')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0       ano   60 non-null     int64  
#   1   variable  60 non-null     object 
#   2   valor     60 non-null     float64
#  
#  |    |       ano | variable   |   valor |
#  |---:|----------:|:-----------|--------:|
#  |  0 |      1992 | Argentina  |     6.2 |
#  
#  ------------------------------
#  
#  rename_cols(map={'    ano': 'anio', 'variable': 'indicador'})
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       60 non-null     int64  
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Argentina   |     6.2 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='AmericaLatina', new_value='América Latina')
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       60 non-null     int64  
#   1   indicador  60 non-null     object 
#   2   valor      60 non-null     float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Argentina   |     6.2 |
#  
#  ------------------------------
#  