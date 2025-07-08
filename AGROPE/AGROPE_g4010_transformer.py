from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='ranking <= 13'),
	query(condition='ncm6 == 100199'),
	sort_values(how='descending', by='share')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1233 entries, 0 to 1232
#  Data columns (total 7 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         1233 non-null   object 
#   1   pais_nombre  1223 non-null   object 
#   2   ncm6         1233 non-null   int64  
#   3   desc_ncm6    1233 non-null   object 
#   4   expo         1233 non-null   float64
#   5   share        1233 non-null   float64
#   6   ranking      1233 non-null   int64  
#  
#  |    | iso3   | pais_nombre   |   ncm6 | desc_ncm6                                           |    expo |   share |   ranking |
#  |---:|:-------|:--------------|-------:|:----------------------------------------------------|--------:|--------:|----------:|
#  |  0 | AUT    | Austria       | 120190 | La soja, incluso quebrantadas, excepto para siembra | 78292.2 |   0.083 |        22 |
#  
#  ------------------------------
#  
#  query(condition='ranking <= 13')
#  Index: 130 entries, 6 to 1209
#  Data columns (total 7 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         130 non-null    object 
#   1   pais_nombre  130 non-null    object 
#   2   ncm6         130 non-null    int64  
#   3   desc_ncm6    130 non-null    object 
#   4   expo         130 non-null    float64
#   5   share        130 non-null    float64
#   6   ranking      130 non-null    int64  
#  
#  |    | iso3   | pais_nombre   |   ncm6 | desc_ncm6                                       |   expo |   share |   ranking |
#  |---:|:-------|:--------------|-------:|:------------------------------------------------|-------:|--------:|----------:|
#  |  6 | BOL    | Bolivia       | 150710 | Aceite de soja "soya" en bruto, incl. desgomado | 497561 |   4.816 |         4 |
#  
#  ------------------------------
#  
#  query(condition='ncm6 == 100199')
#  Index: 13 entries, 49 to 1003
#  Data columns (total 7 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         13 non-null     object 
#   1   pais_nombre  13 non-null     object 
#   2   ncm6         13 non-null     int64  
#   3   desc_ncm6    13 non-null     object 
#   4   expo         13 non-null     float64
#   5   share        13 non-null     float64
#   6   ranking      13 non-null     int64  
#  
#  |    | iso3   | pais_nombre   |   ncm6 | desc_ncm6                                                            |        expo |   share |   ranking |
#  |---:|:-------|:--------------|-------:|:---------------------------------------------------------------------|------------:|--------:|----------:|
#  | 49 | KAZ    | Kazajstán     | 100199 | Otros tipos de trigo y el trigo con centeno, excepto para la siembra | 1.60792e+06 |   3.122 |        11 |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by='share')
#  RangeIndex: 13 entries, 0 to 12
#  Data columns (total 7 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         13 non-null     object 
#   1   pais_nombre  13 non-null     object 
#   2   ncm6         13 non-null     int64  
#   3   desc_ncm6    13 non-null     object 
#   4   expo         13 non-null     float64
#   5   share        13 non-null     float64
#   6   ranking      13 non-null     int64  
#  
#  |    | iso3   | pais_nombre   |   ncm6 | desc_ncm6                                                            |        expo |   share |   ranking |
#  |---:|:-------|:--------------|-------:|:---------------------------------------------------------------------|------------:|--------:|----------:|
#  |  0 | AUS    | Australia     | 100199 | Otros tipos de trigo y el trigo con centeno, excepto para la siembra | 9.21215e+06 |  17.889 |         1 |
#  
#  ------------------------------
#  