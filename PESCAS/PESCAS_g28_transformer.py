from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_value(col='subclase_desc', curr_value='Pescados y mariscos en conserva', new_value='En conserva'),
	replace_value(col='subclase_desc', curr_value='Pescados y mariscos frescos, congelados o semipreparados', new_value='Frescos, congelados o semipreparados')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   dinth_t        20 non-null     int64  
#   1   subclase_desc  20 non-null     object 
#   2   share_gasto    20 non-null     float64
#  
#  |    |   dinth_t | subclase_desc                   |   share_gasto |
#  |---:|----------:|:--------------------------------|--------------:|
#  |  0 |         1 | Pescados y mariscos en conserva |         0.342 |
#  
#  ------------------------------
#  
#  replace_value(col='subclase_desc', curr_value='Pescados y mariscos en conserva', new_value='En conserva')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   dinth_t        20 non-null     int64  
#   1   subclase_desc  20 non-null     object 
#   2   share_gasto    20 non-null     float64
#  
#  |    |   dinth_t | subclase_desc   |   share_gasto |
#  |---:|----------:|:----------------|--------------:|
#  |  0 |         1 | En conserva     |         0.342 |
#  
#  ------------------------------
#  
#  replace_value(col='subclase_desc', curr_value='Pescados y mariscos frescos, congelados o semipreparados', new_value='Frescos, congelados o semipreparados')
#  RangeIndex: 20 entries, 0 to 19
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   dinth_t        20 non-null     int64  
#   1   subclase_desc  20 non-null     object 
#   2   share_gasto    20 non-null     float64
#  
#  |    |   dinth_t | subclase_desc   |   share_gasto |
#  |---:|----------:|:----------------|--------------:|
#  |  0 |         1 | En conserva     |         0.342 |
#  
#  ------------------------------
#  