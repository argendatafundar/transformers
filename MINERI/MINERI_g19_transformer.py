from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_columns(sector='categoria', ciiu_3_4c_desc='subcategoria', perc_total='valor'),
	drop_col(col='monto', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   sector          21 non-null     object 
#   1   ciiu_3_4c_desc  21 non-null     object 
#   2   monto           21 non-null     int64  
#   3   perc_total      21 non-null     float64
#  
#  |    | sector                  | ciiu_3_4c_desc                                                                                 |   monto |   perc_total |
#  |---:|:------------------------|:-----------------------------------------------------------------------------------------------|--------:|-------------:|
#  |  0 | Metales y metalmecánica | Fabricación de maquinaria para la explotación de minas y canteras y para obras de construcción |    3117 |        20.48 |
#  
#  ------------------------------
#  
#  rename_columns(sector='categoria', ciiu_3_4c_desc='subcategoria', perc_total='valor')
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   categoria     21 non-null     object 
#   1   subcategoria  21 non-null     object 
#   2   monto         21 non-null     int64  
#   3   valor         21 non-null     float64
#  
#  |    | categoria               | subcategoria                                                                                   |   monto |   valor |
#  |---:|:------------------------|:-----------------------------------------------------------------------------------------------|--------:|--------:|
#  |  0 | Metales y metalmecánica | Fabricación de maquinaria para la explotación de minas y canteras y para obras de construcción |    3117 |   20.48 |
#  
#  ------------------------------
#  
#  drop_col(col='monto', axis=1)
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   categoria     21 non-null     object 
#   1   subcategoria  21 non-null     object 
#   2   valor         21 non-null     float64
#  
#  |    | categoria               | subcategoria                                                                                   |   valor |
#  |---:|:------------------------|:-----------------------------------------------------------------------------------------------|--------:|
#  |  0 | Metales y metalmecánica | Fabricación de maquinaria para la explotación de minas y canteras y para obras de construcción |   20.48 |
#  
#  ------------------------------
#  