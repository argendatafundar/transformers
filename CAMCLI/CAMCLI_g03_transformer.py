from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geoselector', 'valor_en_ton': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 23288 entries, 0 to 23287
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   iso3          23288 non-null  object 
#   1   anio          23288 non-null  int64  
#   2   valor_en_ton  23288 non-null  float64
#  
#  |    | iso3   |   anio |   valor_en_ton |
#  |---:|:-------|-------:|---------------:|
#  |  0 | AFG    |   1949 |     0.00199215 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geoselector', 'valor_en_ton': 'valor'})
#  RangeIndex: 23288 entries, 0 to 23287
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  23288 non-null  object 
#   1   anio         23288 non-null  int64  
#   2   valor        23288 non-null  float64
#  
#  |    | geoselector   |   anio |      valor |
#  |---:|:--------------|-------:|-----------:|
#  |  0 | AFG           |   1949 | 0.00199215 |
#  
#  ------------------------------
#  