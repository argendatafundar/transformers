from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def imput_value_by_condition(df:DataFrame, bool_mask:list[bool], col:str, value:Any):
    df.loc[bool_mask,col] = value
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	imput_value_by_condition(bool_mask=0       True
1       True
2       True
3       True
4       True
       ...  
462     True
463     True
464    False
465     True
466     True
Name: porcentaje, Length: 467, dtype: bool, col='programa_desc', value='Otros')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 467 entries, 0 to 466
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   ppg_label      467 non-null    object 
#   1   programa_desc  467 non-null    object 
#   2   porcentaje     467 non-null    float64
#  
#  |    | ppg_label   | programa_desc                                                               |   porcentaje |
#  |---:|:------------|:----------------------------------------------------------------------------|-------------:|
#  |  0 | No PPG      | Abordaje Integral de Enfermedades No Transmisibles y sus Factores de Riesgo |  0.000362015 |
#  
#  ------------------------------
#  
#  imput_value_by_condition(bool_mask=0       True
#  1       True
#  2       True
#  3       True
#  4       True
#         ...  
#  462     True
#  463     True
#  464    False
#  465     True
#  466     True
#  Name: porcentaje, Length: 467, dtype: bool, col='programa_desc', value='Otros')
#  RangeIndex: 467 entries, 0 to 466
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   ppg_label      467 non-null    object 
#   1   programa_desc  467 non-null    object 
#   2   porcentaje     467 non-null    float64
#  
#  |    | ppg_label   | programa_desc   |   porcentaje |
#  |---:|:------------|:----------------|-------------:|
#  |  0 | No PPG      | Otros           |  0.000362015 |
#  
#  ------------------------------
#  