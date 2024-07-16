from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
# break lines in strings based on nchars
def break_lines(df: DataFrame, col:str, nchars:int, break_words:bool = False):
    df[col] = df[col].str.wrap(nchars, break_long_words = break_words)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'rama_vendedora': 'serie', 'porcentaje_compra_sector_minero': 'valor'}),
	break_lines(col='serie', nchars=10, break_words=False)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 2 columns):
#   #   Column                           Non-Null Count  Dtype  
#  ---  ------                           --------------  -----  
#   0   rama_vendedora                   15 non-null     object 
#   1   porcentaje_compra_sector_minero  15 non-null     float64
#  
#  |    | rama_vendedora   |   porcentaje_compra_sector_minero |
#  |---:|:-----------------|----------------------------------:|
#  |  0 | Industria        |                                24 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rama_vendedora': 'serie', 'porcentaje_compra_sector_minero': 'valor'})
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   serie   15 non-null     object 
#   1   valor   15 non-null     float64
#  
#  |    | serie     |   valor |
#  |---:|:----------|--------:|
#  |  0 | Industria |      24 |
#  
#  ------------------------------
#  
#  break_lines(col='serie', nchars=10, break_words=False)
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 2 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   serie   15 non-null     object 
#   1   valor   15 non-null     float64
#  
#  |    | serie     |   valor |
#  |---:|:----------|--------:|
#  |  0 | Industria |      24 |
#  
#  ------------------------------
#  