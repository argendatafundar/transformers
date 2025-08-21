from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    data = df.copy()
    data[col] = data[col]*k
    return data

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	multiplicar_por_escalar(col='valor', k=100),
	query(condition="codigo == '3.1.1.2.04'"),
	query(condition="variable == 'Participación en la concentración'")
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
#  multiplicar_por_escalar(col='valor', k=100)
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
#  |    |   decil | codigo   |   desagregacion | gran_categoria             | categoria   | subcategoria       | rubro   | instrumento                                             | variable                                       |    valor |
#  |---:|--------:|:---------|----------------:|:---------------------------|:------------|:-------------------|:--------|:--------------------------------------------------------|:-----------------------------------------------|---------:|
#  |  0 |       1 | 2.1.1.01 |               9 | Impuestos y contribuciones | Impuestos   | Impuestos directos |         | Impuestos a la nómina salarial (PAMI, sindicatos, etc.) | Concentración per cápita (en dólares PPP 2011) | -0.31167 |
#  
#  ------------------------------
#  
#  query(condition="codigo == '3.1.1.2.04'")
#  Index: 30 entries, 390 to 419
#  Data columns (total 10 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   decil           30 non-null     int64  
#   1   codigo          30 non-null     object 
#   2   desagregacion   30 non-null     int64  
#   3   gran_categoria  30 non-null     object 
#   4   categoria       30 non-null     object 
#   5   subcategoria    30 non-null     object 
#   6   rubro           30 non-null     object 
#   7   instrumento     30 non-null     object 
#   8   variable        30 non-null     object 
#   9   valor           30 non-null     float64
#  
#  |     |   decil | codigo     |   desagregacion | gran_categoria             | categoria      | subcategoria              | rubro            | instrumento                         | variable                                       |   valor |
#  |----:|--------:|:-----------|----------------:|:---------------------------|:---------------|:--------------------------|:-----------------|:------------------------------------|:-----------------------------------------------|--------:|
#  | 390 |       1 | 3.1.1.2.04 |               9 | Transferencias y subsidios | Transferencias | Transferencias monetarias | No contributivas | Asignación Universal por Hijo (AUH) | Concentración per cápita (en dólares PPP 2011) | 86.0282 |
#  
#  ------------------------------
#  
#  query(condition="variable == 'Participación en la concentración'")
#  Index: 10 entries, 410 to 419
#  Data columns (total 10 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   decil           10 non-null     int64  
#   1   codigo          10 non-null     object 
#   2   desagregacion   10 non-null     int64  
#   3   gran_categoria  10 non-null     object 
#   4   categoria       10 non-null     object 
#   5   subcategoria    10 non-null     object 
#   6   rubro           10 non-null     object 
#   7   instrumento     10 non-null     object 
#   8   variable        10 non-null     object 
#   9   valor           10 non-null     float64
#  
#  |     |   decil | codigo     |   desagregacion | gran_categoria             | categoria      | subcategoria              | rubro            | instrumento                         | variable                          |   valor |
#  |----:|--------:|:-----------|----------------:|:---------------------------|:---------------|:--------------------------|:-----------------|:------------------------------------|:----------------------------------|--------:|
#  | 410 |       1 | 3.1.1.2.04 |               9 | Transferencias y subsidios | Transferencias | Transferencias monetarias | No contributivas | Asignación Universal por Hijo (AUH) | Participación en la concentración | 28.4378 |
#  
#  ------------------------------
#  