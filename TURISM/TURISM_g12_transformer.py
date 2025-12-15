from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_categorica(df, col_cat:str, order_cat:list[str]):
    import pandas as pd
    df[col_cat] = pd.Categorical(df[col_cat], categories=order_cat, ordered=True)
    return df.sort_values(by=[col_cat])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	ordenar_categorica(col_cat='rama_etq', order_cat=['Gastronomía', 'Transporte de pasajeros', 'Arte, recreación y cultura', 'Agencias de viaje', 'Alojamiento', 'Resto de actividades'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6 entries, 0 to 5
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   rama_etq  6 non-null      object 
#   1   emp       6 non-null      int64  
#   2   prop      6 non-null      float64
#  
#  |    | rama_etq    |    emp |    prop |
#  |---:|:------------|-------:|--------:|
#  |  0 | Gastronomía | 871409 | 44.5475 |
#  
#  ------------------------------
#  
#  ordenar_categorica(col_cat='rama_etq', order_cat=['Gastronomía', 'Transporte de pasajeros', 'Arte, recreación y cultura', 'Agencias de viaje', 'Alojamiento', 'Resto de actividades'])
#  Index: 6 entries, 0 to 4
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype   
#  ---  ------    --------------  -----   
#   0   rama_etq  6 non-null      category
#   1   emp       6 non-null      int64   
#   2   prop      6 non-null      float64 
#  
#  |    | rama_etq    |    emp |    prop |
#  |---:|:------------|-------:|--------:|
#  |  0 | Gastronomía | 871409 | 44.5475 |
#  
#  ------------------------------
#  