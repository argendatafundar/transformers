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

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tasa_desocupacion': 'valor', 'nivel_ed_desc': 'indicador'}),
	rename_cols(map={'indicador': 'categoria'}),
	drop_col(col='nivel_ed_cod', axis=1),
	mutiplicar_por_escalar(col='valor', k=100)
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
#  |  0 |   2003 |              1 | Hasta secundaria incompleta |            0.119657 |
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
#  |  0 |   2003 |              1 | Hasta secundaria incompleta | 0.119657 |
#  
#  ------------------------------
#  
#  rename_cols(map={'indicador': 'categoria'})
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          84 non-null     int64  
#   1   nivel_ed_cod  84 non-null     int64  
#   2   categoria     84 non-null     object 
#   3   valor         84 non-null     float64
#  
#  |    |   anio |   nivel_ed_cod | categoria                   |    valor |
#  |---:|-------:|---------------:|:----------------------------|---------:|
#  |  0 |   2003 |              1 | Hasta secundaria incompleta | 0.119657 |
#  
#  ------------------------------
#  
#  drop_col(col='nivel_ed_cod', axis=1)
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       84 non-null     int64  
#   1   categoria  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    |   anio | categoria                   |   valor |
#  |---:|-------:|:----------------------------|--------:|
#  |  0 |   2003 | Hasta secundaria incompleta | 11.9657 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       84 non-null     int64  
#   1   categoria  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    |   anio | categoria                   |   valor |
#  |---:|-------:|:----------------------------|--------:|
#  |  0 |   2003 | Hasta secundaria incompleta | 11.9657 |
#  
#  ------------------------------
#  