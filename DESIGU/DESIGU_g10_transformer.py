from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

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
	query(condition="variable in ('giniila','brecha')"),
	long_to_wide(index=['ano'], columns='variable', values='valor')
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
#  query(condition="variable in ('giniila','brecha')")
#  Index: 66 entries, 0 to 65
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
#  long_to_wide(index=['ano'], columns='variable', values='valor')
#  RangeIndex: 33 entries, 0 to 32
#  Data columns (total 3 columns):
#   #   Column   Non-Null Count  Dtype  
#  ---  ------   --------------  -----  
#   0   ano      33 non-null     int64  
#   1   brecha   33 non-null     float64
#   2   giniila  33 non-null     float64
#  
#  |    |   ano |   brecha |   giniila |
#  |---:|------:|---------:|----------:|
#  |  0 |  1992 |     1.96 |     40.11 |
#  
#  ------------------------------
#  