from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'ano': 'anio', 'variable': 'indicador'}),
	query(condition="indicador == 'participacion'")
)
#  PIPELINE_END


#  start()
#  RangeIndex: 172 entries, 0 to 171
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       172 non-null    int64  
#   1   variable  172 non-null    object 
#   2   valor     172 non-null    float64
#  
#  |    |   ano | variable      |   valor |
#  |---:|------:|:--------------|--------:|
#  |  0 |  1935 | participacion |    35.5 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'indicador'})
#  RangeIndex: 172 entries, 0 to 171
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       172 non-null    int64  
#   1   indicador  172 non-null    object 
#   2   valor      172 non-null    float64
#  
#  |    |   anio | indicador     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1935 | participacion |    35.5 |
#  
#  ------------------------------
#  
#  query(condition="indicador == 'participacion'")
#  Index: 86 entries, 0 to 170
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       86 non-null     int64  
#   1   indicador  86 non-null     object 
#   2   valor      86 non-null     float64
#  
#  |    |   anio | indicador     |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1935 | participacion |    35.5 |
#  
#  ------------------------------
#  