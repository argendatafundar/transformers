from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: DataFrame) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
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
#  identity()
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