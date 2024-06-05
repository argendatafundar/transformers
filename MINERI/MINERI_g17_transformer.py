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
rename_cols(map={'destino': 'serie', 'concepto': 'subsubserie', 'porcentaje_total': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   destino           16 non-null     object 
#   1   concepto          16 non-null     object 
#   2   porcentaje_total  16 non-null     float64
#  
#  |    | destino   | concepto                                                       |   porcentaje_total |
#  |---:|:----------|:---------------------------------------------------------------|-------------------:|
#  |  0 | local     | consumo intermedio nacional (neto de importaciones indirectas) |               29.8 |
#  
#  ------------------------------
#  
#  rename_cols(map={'destino': 'serie', 'concepto': 'subsubserie', 'porcentaje_total': 'valor'})
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   serie        16 non-null     object 
#   1   subsubserie  16 non-null     object 
#   2   valor        16 non-null     float64
#  
#  |    | serie   | subsubserie                                                    |   valor |
#  |---:|:--------|:---------------------------------------------------------------|--------:|
#  |  0 | local   | consumo intermedio nacional (neto de importaciones indirectas) |    29.8 |
#  
#  ------------------------------
#  