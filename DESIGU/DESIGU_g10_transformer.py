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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'ano': 'anio', 'variable': 'categoria'}),
	replace_value(col='categoria', curr_value='giniila', new_value='Gini ingresos laborales'),
	replace_value(col='categoria', curr_value='brecha', new_value='Brecha salarial calificados/no calificados')
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
#  |  0 |  1992 | giniila    |   40.11 |
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
#  |  0 |   1992 | giniila     |   40.11 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='giniila', new_value='Gini ingresos laborales')
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       66 non-null     int64  
#   1   categoria  66 non-null     object 
#   2   valor      66 non-null     float64
#  
#  |    |   anio | categoria               |   valor |
#  |---:|-------:|:------------------------|--------:|
#  |  0 |   1992 | Gini ingresos laborales |   40.11 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='brecha', new_value='Brecha salarial calificados/no calificados')
#  RangeIndex: 66 entries, 0 to 65
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       66 non-null     int64  
#   1   categoria  66 non-null     object 
#   2   valor      66 non-null     float64
#  
#  |    |   anio | categoria               |   valor |
#  |---:|-------:|:------------------------|--------:|
#  |  0 |   1992 | Gini ingresos laborales |   40.11 |
#  
#  ------------------------------
#  