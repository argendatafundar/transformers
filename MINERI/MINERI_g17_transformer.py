from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(destino='nivel1', concepto='nivel2', porcentaje_total='valor')
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
#  rename_columns(destino='nivel1', concepto='nivel2', porcentaje_total='valor')
#  RangeIndex: 16 entries, 0 to 15
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  16 non-null     object 
#   1   nivel2  16 non-null     object 
#   2   valor   16 non-null     float64
#  
#  |    | nivel1   | nivel2                                                         |   valor |
#  |---:|:---------|:---------------------------------------------------------------|--------:|
#  |  0 | local    | consumo intermedio nacional (neto de importaciones indirectas) |    29.8 |
#  
#  ------------------------------
#  