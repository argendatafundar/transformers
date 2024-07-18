from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'sector': 'indicador'}),
	replace_value(col='indicador', curr_value='Construccion y Electricidad, Gas y Agua', new_value='Construccion y EGA')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 440 entries, 0 to 439
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    440 non-null    int64  
#   1   sector  440 non-null    object 
#   2   valor   440 non-null    float64
#  
#  |    |   anio | sector                                |   valor |
#  |---:|-------:|:--------------------------------------|--------:|
#  |  0 |   1935 | Agricultura caza silvicultura y pesca |  25.865 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador'})
#  RangeIndex: 440 entries, 0 to 439
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       440 non-null    int64  
#   1   indicador  440 non-null    object 
#   2   valor      440 non-null    float64
#  
#  |    |   anio | indicador                             |   valor |
#  |---:|-------:|:--------------------------------------|--------:|
#  |  0 |   1935 | Agricultura caza silvicultura y pesca |  25.865 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Construccion y Electricidad, Gas y Agua', new_value='Construccion y EGA')
#  RangeIndex: 440 entries, 0 to 439
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       440 non-null    int64  
#   1   indicador  440 non-null    object 
#   2   valor      440 non-null    float64
#  
#  |    |   anio | indicador                             |   valor |
#  |---:|-------:|:--------------------------------------|--------:|
#  |  0 |   1935 | Agricultura caza silvicultura y pesca |  25.865 |
#  
#  ------------------------------
#  