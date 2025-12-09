from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 5403 entries, 0 to 5402
#  Data columns (total 6 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   anio                        5403 non-null   int64  
#   1   geocodigoFundar             5403 non-null   object 
#   2   geonombreFundar             5403 non-null   object 
#   3   int_tourism_receipts        5304 non-null   float64
#   4   int_tourism_receipts_share  5304 non-null   float64
#   5   origen                      5403 non-null   object 
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   int_tourism_receipts |   int_tourism_receipts_share | origen   |
#  |---:|-------:|:------------------|:------------------|-----------------------:|-----------------------------:|:---------|
#  |  0 |   1995 | ABW               | Aruba             |                0.49619 |                     0.115351 | empalme  |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 147 entries, 29 to 5052
#  Data columns (total 6 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   anio                        147 non-null    int64  
#   1   geocodigoFundar             147 non-null    object 
#   2   geonombreFundar             147 non-null    object 
#   3   int_tourism_receipts        146 non-null    float64
#   4   int_tourism_receipts_share  146 non-null    float64
#   5   origen                      147 non-null    object 
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   |   int_tourism_receipts |   int_tourism_receipts_share | origen   |
#  |---:|-------:|:------------------|:------------------|-----------------------:|-----------------------------:|:---------|
#  | 29 |   2024 | ABW               | Aruba             |                      3 |                     0.172911 | empalme  |
#  
#  ------------------------------
#  