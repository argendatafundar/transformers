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
	query(condition='ranking <= 10'),
	query(condition='ncm6 == 20130'),
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
#  query(condition='ranking <= 10')
#  Index: 100 entries, 6 to 1209
#  Data columns (total 7 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         100 non-null    object 
#   1   pais_nombre  100 non-null    object 
#   2   ncm6         100 non-null    int64  
#   3   desc_ncm6    100 non-null    object 
#   4   expo         100 non-null    float64
#   5   share        100 non-null    float64
#   6   ranking      100 non-null    int64  
#  
#  |    | iso3   | pais_nombre   |   ncm6 | desc_ncm6                                       |   expo |   share |   ranking |
#  |---:|:-------|:--------------|-------:|:------------------------------------------------|-------:|--------:|----------:|
#  |  6 | BOL    | Bolivia       | 150710 | Aceite de soja "soya" en bruto, incl. desgomado | 497561 |   4.816 |         4 |
#  
#  ------------------------------
#  
#  query(condition='ncm6 == 20130')
#  Index: 10 entries, 67 to 1141
#  Data columns (total 7 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         10 non-null     object 
#   1   pais_nombre  10 non-null     object 
#   2   ncm6         10 non-null     int64  
#   3   desc_ncm6    10 non-null     object 
#   4   expo         10 non-null     float64
#   5   share        10 non-null     float64
#   6   ranking      10 non-null     int64  
#  
#  |    | iso3   | pais_nombre   |   ncm6 | desc_ncm6                                          |   expo |   share |   ranking |
#  |---:|:-------|:--------------|-------:|:---------------------------------------------------|-------:|--------:|----------:|
#  | 67 | POL    | Polonia       |  20130 | Carne deshuesada, de bovinos, fresca o refrigerada | 809310 |    3.78 |         9 |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by='share')
#  RangeIndex: 10 entries, 0 to 9
#  Data columns (total 7 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   iso3         10 non-null     object 
#   1   pais_nombre  10 non-null     object 
#   2   ncm6         10 non-null     int64  
#   3   desc_ncm6    10 non-null     object 
#   4   expo         10 non-null     float64
#   5   share        10 non-null     float64
#   6   ranking      10 non-null     int64  
#  
#  |    | iso3   | pais_nombre    |   ncm6 | desc_ncm6                                          |        expo |   share |   ranking |
#  |---:|:-------|:---------------|-------:|:---------------------------------------------------|------------:|--------:|----------:|
#  |  0 | USA    | Estados Unidos |  20130 | Carne deshuesada, de bovinos, fresca o refrigerada | 4.03086e+06 |  18.827 |         1 |
#  
#  ------------------------------
#  