from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'rama_actividad': 'categoria', 'categoria_ocupacional': 'indicador', 'porcentaje_sobre_total_rama': 'valor'}),
	replace_values(col='indicador', values={'asalariados_registrados': 'Asalariados registrados', 'asalariados_no_registrados': 'Asalariados no registrados', 'no_asalariados': 'No asalariados'}),
	ordenar_dos_columnas(col1='categoria', order1=['Construcción', 'Serv. Doméstico', 'Otros servicios', 'Act. profesionales, científicas y técnicas', 'Comercio', 'Agro', 'Inmobiliarias', 'Hoteles y restaurantes', 'Recreación', 'Transporte', 'Promedio ocupados', 'Industria', 'Act. Administrativas', 'Salud', 'Información y comunicación', 'Reciclamiento de desperdicios, agua y saneamiento', 'Otras minas y canteras', 'Finanzas', 'Enseñanza', 'Administración pública y defensa', 'Electricidad y gas', 'Extracción de petróleo y gas', 'Minería metalífera'], col2='indicador', order2=['Asalariados registrados', 'Asalariados no registrados', 'No asalariados'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   rama_actividad               69 non-null     object 
#   1   categoria_ocupacional        69 non-null     object 
#   2   porcentaje_sobre_total_rama  69 non-null     float64
#  
#  |    | rama_actividad   | categoria_ocupacional   |   porcentaje_sobre_total_rama |
#  |---:|:-----------------|:------------------------|------------------------------:|
#  |  0 | Construcción     | asalariados_registrados |                         15.91 |
#  
#  ------------------------------
#  
#  rename_cols(map={'rama_actividad': 'categoria', 'categoria_ocupacional': 'indicador', 'porcentaje_sobre_total_rama': 'valor'})
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  69 non-null     object 
#   1   indicador  69 non-null     object 
#   2   valor      69 non-null     float64
#  
#  |    | categoria    | indicador               |   valor |
#  |---:|:-------------|:------------------------|--------:|
#  |  0 | Construcción | asalariados_registrados |   15.91 |
#  
#  ------------------------------
#  
#  replace_values(col='indicador', values={'asalariados_registrados': 'Asalariados registrados', 'asalariados_no_registrados': 'Asalariados no registrados', 'no_asalariados': 'No asalariados'})
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  69 non-null     category
#   1   indicador  69 non-null     category
#   2   valor      69 non-null     float64 
#  
#  |    | categoria    | indicador               |   valor |
#  |---:|:-------------|:------------------------|--------:|
#  |  0 | Construcción | Asalariados registrados |   15.91 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='categoria', order1=['Construcción', 'Serv. Doméstico', 'Otros servicios', 'Act. profesionales, científicas y técnicas', 'Comercio', 'Agro', 'Inmobiliarias', 'Hoteles y restaurantes', 'Recreación', 'Transporte', 'Promedio ocupados', 'Industria', 'Act. Administrativas', 'Salud', 'Información y comunicación', 'Reciclamiento de desperdicios, agua y saneamiento', 'Otras minas y canteras', 'Finanzas', 'Enseñanza', 'Administración pública y defensa', 'Electricidad y gas', 'Extracción de petróleo y gas', 'Minería metalífera'], col2='indicador', order2=['Asalariados registrados', 'Asalariados no registrados', 'No asalariados'])
#  RangeIndex: 69 entries, 0 to 68
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype   
#  ---  ------     --------------  -----   
#   0   categoria  69 non-null     category
#   1   indicador  69 non-null     category
#   2   valor      69 non-null     float64 
#  
#  |    | categoria    | indicador               |   valor |
#  |---:|:-------------|:------------------------|--------:|
#  |  0 | Construcción | Asalariados registrados |   15.91 |
#  
#  ------------------------------
#  