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
#  RangeIndex: 336 entries, 0 to 335
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          336 non-null    int64  
#   1   provincia_id  336 non-null    int64  
#   2   provincia     336 non-null    object 
#   3   tgf           336 non-null    float64
#  
#  |    |   anio |   provincia_id | provincia   |    tgf |
#  |---:|-------:|---------------:|:------------|-------:|
#  |  0 |   2010 |              2 | CABA        | 1.9359 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 336 entries, 0 to 335
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          336 non-null    int64  
#   1   provincia_id  336 non-null    int64  
#   2   provincia     336 non-null    object 
#   3   tgf           336 non-null    float64
#  
#  |    |   anio |   provincia_id | provincia   |    tgf |
#  |---:|-------:|---------------:|:------------|-------:|
#  |  0 |   2010 |              2 | CABA        | 1.9359 |
#  
#  ------------------------------
#  