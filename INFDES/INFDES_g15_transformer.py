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
rename_cols(map={'tasa_desocupacion': 'valor', 'nivel_ed_desc': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   anio               84 non-null     int64  
#   1   nivel_ed_cod       84 non-null     int64  
#   2   nivel_ed_desc      84 non-null     object 
#   3   tasa_desocupacion  84 non-null     float64
#  
#  |    |   anio |   nivel_ed_cod | nivel_ed_desc               |   tasa_desocupacion |
#  |---:|-------:|---------------:|:----------------------------|--------------------:|
#  |  0 |   2003 |              1 | Hasta secundaria incompleta |            0.119194 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tasa_desocupacion': 'valor', 'nivel_ed_desc': 'indicador'})
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          84 non-null     int64  
#   1   nivel_ed_cod  84 non-null     int64  
#   2   indicador     84 non-null     object 
#   3   valor         84 non-null     float64
#  
#  |    |   anio |   nivel_ed_cod | indicador                   |    valor |
#  |---:|-------:|---------------:|:----------------------------|---------:|
#  |  0 |   2003 |              1 | Hasta secundaria incompleta | 0.119194 |
#  
#  ------------------------------
#  