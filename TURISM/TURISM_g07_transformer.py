from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy

@transformer.convert
def custom_string_funcion(df:DataFrame): 
    df['categoria2'] = df['categoria'].str.removeprefix(prefix="Complejo ").apply(lambda x: x[0].upper() + x[1:])
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	custom_string_funcion(),
	sort_values(how='descending', by='exportaciones_en_usd_mill'),
	replace_multiple_values(col='categoria2', replacements={'Soja': 'Soja', 'Petrolero-petroquímico': 'Petrolero-petroq.', 'Servicios basados en conocimiento': 'SBC', 'Automotriz': 'Automotriz', 'Maicero': 'Maíz', 'Resto de exportaciones': 'Resto exp.', 'Viajes': 'Turismo', 'Oro y plata': 'Oro y plata', 'Carne y cuero bovinos': 'Carne/cuero bov.', 'Triguero': 'Trigo', 'Transporte': 'Transporte', 'Pesquero': 'Pesquero', 'Girasol': 'Girasol', 'Lácteo': 'Lácteo', 'Cebada': 'Cebada', 'Maní': 'Maní', 'Farmacéutico': 'Farmacéutico', 'Siderúrgico': 'Siderúrgico', 'Aluminio': 'Aluminio', 'Uva': 'Uva', 'Forestal': 'Forestal', 'Litio': 'Litio', 'Limón': 'Limón', 'Azucarero': 'Azúcar', 'Textil': 'Textil', 'Peras y manzanas': 'Peras/manzanas', 'Papa': 'Papa', 'Tabacalero': 'Tabaco', 'Olivícola': 'Olivo', 'Avícola': 'Avícola', 'Bienes y serv. del gobierno, n.i.o.p.': 'Bienes/serv. del gob.', 'Arrocero': 'Arroz', 'Porotos': 'Porotos', 'Resto del sector frutícola': 'Resto frutícola', 'Miel': 'Miel', 'Serv. financieros': 'Serv. financieros', 'Ajo': 'Ajo', 'Resto del sector hortícola': 'Resto hortícola', 'Cítricos, excluido el limón': 'Cítricos s/limón', 'Yerba mate': 'Yerba mate', 'Serv. de mantenimiento y reparaciones n.i.o.p.': 'Mant./reparaciones', 'Té': 'Té', 'Garbanzos': 'Garbanzos', 'Arándanos y frutos similares': 'Arándanos', 'Equino': 'Equino', 'Serv. de seguros y pensiones': 'Seguros/pensiones', 'Plomo': 'Plomo', 'Otros minerales metalíferos': 'Otros minerales metalif.', 'Construcción': 'Construcción', 'Serv. de manufactura sobre insumos físicos pertenecientes a otros': 'Manuf. c/insumos ajenos'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 885 entries, 0 to 884
#  Data columns (total 4 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   categoria                  885 non-null    object 
#   1   anio                       885 non-null    int64  
#   2   exportaciones_en_usd_mill  885 non-null    float64
#   3   tipo                       885 non-null    object 
#  
#  |    | categoria             |   anio |   exportaciones_en_usd_mill | tipo   |
#  |---:|:----------------------|-------:|----------------------------:|:-------|
#  |  0 | Complejos oleaginosos |   2006 |                        9770 | Bienes |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 50 entries, 553 to 884
#  Data columns (total 5 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   categoria                  50 non-null     object 
#   1   anio                       50 non-null     int64  
#   2   exportaciones_en_usd_mill  50 non-null     float64
#   3   tipo                       50 non-null     object 
#   4   categoria2                 50 non-null     object 
#  
#  |     | categoria     |   anio |   exportaciones_en_usd_mill | tipo   | categoria2   |
#  |----:|:--------------|-------:|----------------------------:|:-------|:-------------|
#  | 553 | Complejo soja |   2024 |                     19628.1 | Bienes | Soja         |
#  
#  ------------------------------
#  
#  custom_string_funcion()
#  Index: 50 entries, 553 to 884
#  Data columns (total 5 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   categoria                  50 non-null     object 
#   1   anio                       50 non-null     int64  
#   2   exportaciones_en_usd_mill  50 non-null     float64
#   3   tipo                       50 non-null     object 
#   4   categoria2                 50 non-null     object 
#  
#  |     | categoria     |   anio |   exportaciones_en_usd_mill | tipo   | categoria2   |
#  |----:|:--------------|-------:|----------------------------:|:-------|:-------------|
#  | 553 | Complejo soja |   2024 |                     19628.1 | Bienes | Soja         |
#  
#  ------------------------------
#  
#  sort_values(how='descending', by='exportaciones_en_usd_mill')
#  RangeIndex: 50 entries, 0 to 49
#  Data columns (total 5 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   categoria                  50 non-null     object 
#   1   anio                       50 non-null     int64  
#   2   exportaciones_en_usd_mill  50 non-null     float64
#   3   tipo                       50 non-null     object 
#   4   categoria2                 50 non-null     object 
#  
#  |    | categoria     |   anio |   exportaciones_en_usd_mill | tipo   | categoria2   |
#  |---:|:--------------|-------:|----------------------------:|:-------|:-------------|
#  |  0 | Complejo soja |   2024 |                     19628.1 | Bienes | Soja         |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='categoria2', replacements={'Soja': 'Soja', 'Petrolero-petroquímico': 'Petrolero-petroq.', 'Servicios basados en conocimiento': 'SBC', 'Automotriz': 'Automotriz', 'Maicero': 'Maíz', 'Resto de exportaciones': 'Resto exp.', 'Viajes': 'Turismo', 'Oro y plata': 'Oro y plata', 'Carne y cuero bovinos': 'Carne/cuero bov.', 'Triguero': 'Trigo', 'Transporte': 'Transporte', 'Pesquero': 'Pesquero', 'Girasol': 'Girasol', 'Lácteo': 'Lácteo', 'Cebada': 'Cebada', 'Maní': 'Maní', 'Farmacéutico': 'Farmacéutico', 'Siderúrgico': 'Siderúrgico', 'Aluminio': 'Aluminio', 'Uva': 'Uva', 'Forestal': 'Forestal', 'Litio': 'Litio', 'Limón': 'Limón', 'Azucarero': 'Azúcar', 'Textil': 'Textil', 'Peras y manzanas': 'Peras/manzanas', 'Papa': 'Papa', 'Tabacalero': 'Tabaco', 'Olivícola': 'Olivo', 'Avícola': 'Avícola', 'Bienes y serv. del gobierno, n.i.o.p.': 'Bienes/serv. del gob.', 'Arrocero': 'Arroz', 'Porotos': 'Porotos', 'Resto del sector frutícola': 'Resto frutícola', 'Miel': 'Miel', 'Serv. financieros': 'Serv. financieros', 'Ajo': 'Ajo', 'Resto del sector hortícola': 'Resto hortícola', 'Cítricos, excluido el limón': 'Cítricos s/limón', 'Yerba mate': 'Yerba mate', 'Serv. de mantenimiento y reparaciones n.i.o.p.': 'Mant./reparaciones', 'Té': 'Té', 'Garbanzos': 'Garbanzos', 'Arándanos y frutos similares': 'Arándanos', 'Equino': 'Equino', 'Serv. de seguros y pensiones': 'Seguros/pensiones', 'Plomo': 'Plomo', 'Otros minerales metalíferos': 'Otros minerales metalif.', 'Construcción': 'Construcción', 'Serv. de manufactura sobre insumos físicos pertenecientes a otros': 'Manuf. c/insumos ajenos'})
#  RangeIndex: 50 entries, 0 to 49
#  Data columns (total 5 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   categoria                  50 non-null     object 
#   1   anio                       50 non-null     int64  
#   2   exportaciones_en_usd_mill  50 non-null     float64
#   3   tipo                       50 non-null     object 
#   4   categoria2                 50 non-null     object 
#  
#  |    | categoria     |   anio |   exportaciones_en_usd_mill | tipo   | categoria2   |
#  |---:|:--------------|-------:|----------------------------:|:-------|:-------------|
#  |  0 | Complejo soja |   2024 |                     19628.1 | Bienes | Soja         |
#  
#  ------------------------------
#  