from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tipo_energia': 'indicador', 'valor_en_mw': 'valor', 'region': 'categoria'}),
	drop_col(col='porcentaje', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   region        36 non-null     object 
#   1   tipo_energia  36 non-null     object 
#   2   valor_en_mw   36 non-null     int64  
#   3   porcentaje    36 non-null     float64
#  
#  |    | region                           | tipo_energia   |   valor_en_mw |   porcentaje |
#  |---:|:---------------------------------|:---------------|--------------:|-------------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía     |            48 |      3.21932 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_mw': 'valor', 'region': 'categoria'})
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   categoria   36 non-null     object 
#   1   indicador   36 non-null     object 
#   2   valor       36 non-null     int64  
#   3   porcentaje  36 non-null     float64
#  
#  |    | categoria                        | indicador   |   valor |   porcentaje |
#  |---:|:---------------------------------|:------------|--------:|-------------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía  |      48 |      3.21932 |
#  
#  ------------------------------
#  
#  drop_col(col='porcentaje', axis=1)
#  RangeIndex: 36 entries, 0 to 35
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   categoria  36 non-null     object
#   1   indicador  36 non-null     object
#   2   valor      36 non-null     int64 
#  
#  |    | categoria                        | indicador   |   valor |
#  |---:|:---------------------------------|:------------|--------:|
#  |  0 | CABA y Provincia de Buenos Aires | Bioenergía  |      48 |
#  
#  ------------------------------
#  