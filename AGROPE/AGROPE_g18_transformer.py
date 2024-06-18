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
rename_cols(map={'grupo_carne': 'indicador'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   anio         360 non-null    int64  
#   1   grupo_carne  360 non-null    object 
#   2   valor        360 non-null    float64
#  
#  |    |   anio | grupo_carne   |   valor |
#  |---:|-------:|:--------------|--------:|
#  |  0 |   1961 | aviar         | 2.07759 |
#  
#  ------------------------------
#  
#  rename_cols(map={'grupo_carne': 'indicador'})
#  RangeIndex: 360 entries, 0 to 359
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       360 non-null    int64  
#   1   indicador  360 non-null    object 
#   2   valor      360 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1961 | aviar       | 2.07759 |
#  
#  ------------------------------
#  