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
#  RangeIndex: 990 entries, 0 to 989
#  Data columns (total 10 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   decil           990 non-null    int64  
#   1   codigo          990 non-null    object 
#   2   desagregacion   990 non-null    int64  
#   3   gran_categoria  990 non-null    object 
#   4   categoria       990 non-null    object 
#   5   subcategoria    990 non-null    object 
#   6   rubro           990 non-null    object 
#   7   instrumento     990 non-null    object 
#   8   variable        990 non-null    object 
#   9   valor           990 non-null    float64
#  
#  |    |   decil | codigo   |   desagregacion | gran_categoria             | categoria   | subcategoria       | rubro   | instrumento                                             | variable                                       |      valor |
#  |---:|--------:|:---------|----------------:|:---------------------------|:------------|:-------------------|:--------|:--------------------------------------------------------|:-----------------------------------------------|-----------:|
#  |  0 |       1 | 2.1.1.01 |               9 | Impuestos y contribuciones | Impuestos   | Impuestos directos |         | Impuestos a la nómina salarial (PAMI, sindicatos, etc.) | Concentración per cápita (en dólares PPP 2011) | -0.0031167 |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 990 entries, 0 to 989
#  Data columns (total 10 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   decil           990 non-null    int64  
#   1   codigo          990 non-null    object 
#   2   desagregacion   990 non-null    int64  
#   3   gran_categoria  990 non-null    object 
#   4   categoria       990 non-null    object 
#   5   subcategoria    990 non-null    object 
#   6   rubro           990 non-null    object 
#   7   instrumento     990 non-null    object 
#   8   variable        990 non-null    object 
#   9   valor           990 non-null    float64
#  
#  |    |   decil | codigo   |   desagregacion | gran_categoria             | categoria   | subcategoria       | rubro   | instrumento                                             | variable                                       |      valor |
#  |---:|--------:|:---------|----------------:|:---------------------------|:------------|:-------------------|:--------|:--------------------------------------------------------|:-----------------------------------------------|-----------:|
#  |  0 |       1 | 2.1.1.01 |               9 | Impuestos y contribuciones | Impuestos   | Impuestos directos |         | Impuestos a la nómina salarial (PAMI, sindicatos, etc.) | Concentración per cápita (en dólares PPP 2011) | -0.0031167 |
#  
#  ------------------------------
#  