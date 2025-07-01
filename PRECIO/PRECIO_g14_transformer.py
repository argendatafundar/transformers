from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def filtrar_hojas(df, codigo_col="codigo"):

    codigos = set(df[codigo_col].unique())
    padres = set()
    for codigo in codigos:
        if "." in codigo:
            parent = ".".join(codigo.split(".")[:-1])
            padres.add(parent)
    hojas = [codigo for codigo in codigos if codigo not in padres]
    # print(hojas)
    df = df[df[codigo_col].isin(hojas)]
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	filtrar_hojas(codigo_col='codigo')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 748 entries, 0 to 747
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             748 non-null    int64  
#   1   codigo           748 non-null    object 
#   2   nivel            748 non-null    int64  
#   3   rubro            748 non-null    object 
#   4   precio_relativo  748 non-null    float64
#  
#  |    |   anio |   codigo |   nivel | rubro                              |   precio_relativo |
#  |---:|-------:|---------:|--------:|:-----------------------------------|------------------:|
#  |  0 |   2013 |       01 |       1 | Alimentos y bebidas no alcohólicas |               100 |
#  
#  ------------------------------
#  
#  filtrar_hojas(codigo_col='codigo')
#  Index: 561 entries, 2 to 747
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             561 non-null    int64  
#   1   codigo           561 non-null    object 
#   2   nivel            561 non-null    int64  
#   3   rubro            561 non-null    object 
#   4   precio_relativo  561 non-null    float64
#  
#  |    |   anio | codigo   |   nivel | rubro          |   precio_relativo |
#  |---:|-------:|:---------|--------:|:---------------|------------------:|
#  |  2 |   2013 | 01.1.1   |       3 | Pan y cereales |               100 |
#  
#  ------------------------------
#  