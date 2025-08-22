from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'ano': 'anio', 'variable': 'categoria'}),
	replace_value(col='categoria', curr_value='quintil1', new_value='Quintil 1'),
	replace_value(col='categoria', curr_value='quintil5', new_value='Quintil 5'),
	query(condition='anio in [anio.min(), anio.max()]')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       66 non-null     int64  
#   1   variable  66 non-null     object 
#   2   valor     66 non-null     float64
#  
#  |    |   ano | variable   |   valor |
#  |---:|------:|:-----------|--------:|
#  |  0 |  1992 | quintil1   |   2.057 |
#  
#  ------------------------------
#  
#  rename_cols(map={'ano': 'anio', 'variable': 'categoria'})
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       66 non-null     int64  
#   1   categoria  66 non-null     object 
#   2   valor      66 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | quintil1    |   2.057 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='quintil1', new_value='Quintil 1')
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       66 non-null     int64  
#   1   categoria  66 non-null     object 
#   2   valor      66 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Quintil 1   |   2.057 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='quintil5', new_value='Quintil 5')
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       66 non-null     int64  
#   1   categoria  66 non-null     object 
#   2   valor      66 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Quintil 1   |   2.057 |
#  
#  ------------------------------
#  
#  query(condition='anio in [anio.min(), anio.max()]')
#  Index: 4 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       4 non-null      int64  
#   1   categoria  4 non-null      object 
#   2   valor      4 non-null      float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1992 | Quintil 1   |   2.057 |
#  
#  ------------------------------
#  