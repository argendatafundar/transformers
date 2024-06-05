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
rename_cols(map={'region': 'grupo', 'sector': 'indicador', 'valor_en_porcent': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   region            8 non-null      object 
#   1   sector            8 non-null      object 
#   2   valor_en_porcent  8 non-null      float64
#  
#  |    | region    | sector   |   valor_en_porcent |
#  |---:|:----------|:---------|-------------------:|
#  |  0 | Argentina | Energía  |               53.8 |
#  
#  ------------------------------
#  
#  rename_cols(map={'region': 'grupo', 'sector': 'indicador', 'valor_en_porcent': 'valor'})
#  RangeIndex: 8 entries, 0 to 7
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      8 non-null      object 
#   1   indicador  8 non-null      object 
#   2   valor      8 non-null      float64
#  
#  |    | grupo     | indicador   |   valor |
#  |---:|:----------|:------------|--------:|
#  |  0 | Argentina | Energía     |    53.8 |
#  
#  ------------------------------
#  