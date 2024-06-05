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
rename_cols(map={'sector': 'serie', 'ciiu_3_4c_desc': 'subserie'})
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
#  rename_cols(map={'sector': 'serie', 'ciiu_3_4c_desc': 'subserie'})
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   serie       21 non-null     object 
#   1   subserie    21 non-null     object 
#   2   monto       21 non-null     int64  
#   3   perc_total  21 non-null     float64
#  
#  |    | serie                   | subserie                                                                                       |   monto |   perc_total |
#  |---:|:------------------------|:-----------------------------------------------------------------------------------------------|--------:|-------------:|
#  |  0 | Metales y metalmecánica | Fabricación de maquinaria para la explotación de minas y canteras y para obras de construcción |    3117 |        20.48 |
#  
#  ------------------------------
#  