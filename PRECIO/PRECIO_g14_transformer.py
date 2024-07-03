from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

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
rename_cols(map={'rubro': 'indicador'}),
	rename_cols(map={'precio_relativo': 'valor'}),
	query(condition='nivel == 1')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 748 entries, 0 to 747
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             748 non-null    int64  
#   1   codigo           748 non-null    object 
#   2   nivel            748 non-null    int64  
#   3   rubro            748 non-null    object 
#   4   precio_relativo  748 non-null    float64
#  
#  |    |   anio |   codigo |   nivel | rubro                              |   precio_relativo |
#  |---:|-------:|---------:|--------:|:-----------------------------------|------------------:|
#  |  0 |   2013 |       01 |       1 | Alimentos y bebidas no alcohólicas |               100 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rubro': 'indicador'})
#  RangeIndex: 748 entries, 0 to 747
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             748 non-null    int64  
#   1   codigo           748 non-null    object 
#   2   nivel            748 non-null    int64  
#   3   indicador        748 non-null    object 
#   4   precio_relativo  748 non-null    float64
#  
#  |    |   anio |   codigo |   nivel | indicador                          |   precio_relativo |
#  |---:|-------:|---------:|--------:|:-----------------------------------|------------------:|
#  |  0 |   2013 |       01 |       1 | Alimentos y bebidas no alcohólicas |               100 |
#  
#  ------------------------------
#  
#  rename_cols(map={'precio_relativo': 'valor'})
#  RangeIndex: 748 entries, 0 to 747
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       748 non-null    int64  
#   1   codigo     748 non-null    object 
#   2   nivel      748 non-null    int64  
#   3   indicador  748 non-null    object 
#   4   valor      748 non-null    float64
#  
#  |    |   anio |   codigo |   nivel | indicador                          |   valor |
#  |---:|-------:|---------:|--------:|:-----------------------------------|--------:|
#  |  0 |   2013 |       01 |       1 | Alimentos y bebidas no alcohólicas |     100 |
#  
#  ------------------------------
#  
#  query(condition='nivel == 1')
#  Index: 143 entries, 0 to 744
#  Data columns (total 5 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       143 non-null    int64  
#   1   codigo     143 non-null    object 
#   2   nivel      143 non-null    int64  
#   3   indicador  143 non-null    object 
#   4   valor      143 non-null    float64
#  
#  |    |   anio |   codigo |   nivel | indicador                          |   valor |
#  |---:|-------:|---------:|--------:|:-----------------------------------|--------:|
#  |  0 |   2013 |       01 |       1 | Alimentos y bebidas no alcohólicas |     100 |
#  
#  ------------------------------
#  