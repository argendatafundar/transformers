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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'sector': 'indicador'}),
	rename_cols(map={'share_ventas': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          8 non-null      int64  
#   1   sector        8 non-null      object 
#   2   share_ventas  8 non-null      float64
#  
#  |    |   anio | sector   |   share_ventas |
#  |---:|-------:|:---------|---------------:|
#  |  0 |   2014 | SBC      |      0.0259628 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador'})
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          8 non-null      int64  
#   1   indicador     8 non-null      object 
#   2   share_ventas  8 non-null      float64
#  
#  |    |   anio | indicador   |   share_ventas |
#  |---:|-------:|:------------|---------------:|
#  |  0 |   2014 | SBC         |      0.0259628 |
#  
#  ------------------------------
#  
#  rename_cols(map={'share_ventas': 'valor'})
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       8 non-null      int64  
#   1   indicador  8 non-null      object 
#   2   valor      8 non-null      float64
#  
#  |    |   anio | indicador   |     valor |
#  |---:|-------:|:------------|----------:|
#  |  0 |   2014 | SBC         | 0.0259628 |
#  
#  ------------------------------
#  