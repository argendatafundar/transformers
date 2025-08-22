from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def long_to_wide(df:DataFrame, index:list[str], columns:str, values:str):
    df = df.pivot(index=index, columns=columns, values=values).reset_index()
    df.index.name = None
    df.columns.name = None
    df.columns = [str(col) for col in df.columns]  # Convertir columnas a str
    return df  
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	long_to_wide(index=['ano'], columns='variable', values='valor')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 64 entries, 0 to 63
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   ano       64 non-null     int64  
#   1   variable  64 non-null     object 
#   2   valor     64 non-null     float64
#  
#  |    |   ano | variable   |    valor |
#  |---:|------:|:-----------|---------:|
#  |  0 |  1992 | cv         | 0.264629 |
#  
#  ------------------------------
#  
#  long_to_wide(index=['ano'], columns='variable', values='valor')
#  RangeIndex: 32 entries, 0 to 31
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   ano           32 non-null     int64  
#   1   brechagbanea  32 non-null     float64
#   2   cv            32 non-null     float64
#  
#  |    |   ano |   brechagbanea |       cv |
#  |---:|------:|---------------:|---------:|
#  |  0 |  1992 |        1.77956 | 0.264629 |
#  
#  ------------------------------
#  