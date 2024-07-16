from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'sector': 'nivel1', 'ciiu_3_4c_desc': 'nivel2', 'perc_total': 'valor'}),
	drop_col(col='monto', axis=1),
	replace_values(col='nivel2', values={'Fabricación de maquinaria para la explotación de minas y canteras y para obras de construcción': 'Maquinaria para minería, petróleo y construcción', 'Fabricación de sustancias químicas básicas,excepto abonos y compuestos de nitrógeno': 'Sustancias químicas básicas', 'Fabricación de productos de plástico': 'Productos de plástico', 'Fabricación de maquinaria de uso general ncp': 'Maquinarias de uso general', 'Fabricación de bombas, compresores, grifos y válvulas': 'Bombas, compresores y válvulas', 'Fabricación de maquinaria de uso especial ncp': 'Maquinarias de uso especial', 'Fabricación de partes, piezas y accesorios para vehículos automotores y sus motores': 'Partes y piezas para vehículos automotres', 'Fabricación de vehículos automotores': 'Vehículos automotores', 'Fabricación de productos elaborados de metal ncp': 'Otros productos elaborados de metal', 'Fabricación de equipo de elevación y manipulación': 'Equipo de elevación y manipulación', 'Fabricación de cojinetes, engranajes, trenes de engranaje y piezas de transmisión': 'Engranajes y piezas de transmisión', 'Fabricación de productos metálicos para uso estructural y montaje estructural': 'Productos metálicos para uso estructural', 'Fabricación de productos primarios de metales preciosos y metales no ferrosos': 'Productos de metales preciosos y metales no ferrosos', 'Fabricación de cubiertas y cámaras de caucho, recauchutado y renovación de cubiertas de caucho': 'Cubiertas y cámaras de caucho', 'Fabricación de pasta de madera, papel y cartón': 'Pasta de madera, papel y cartón', 'Fabricación de productos de caucho ncp': 'Otros productos de caucho', 'Fabricación de productos químicos ncp': 'Otros productos químicos', 'Industrias básicas de hierro y acero': 'Hierro y acero', 'Fabricación de aparatos de distribución y control de la energía eléctrica': 'Aparatos de control de energía eléctrica', 'Fabricación de artículos de cuchillería, herramientas de mano y artículos de ferretería': 'Artículos de ferretería y herramientas', 'Resto de las actividades': 'Resto de las actividades'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 4 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   sector          21 non-null     object 
#   1   ciiu_3_4c_desc  21 non-null     object 
#   2   monto           21 non-null     int64  
#   3   perc_total      21 non-null     float64
#  
#  |    | sector                  | ciiu_3_4c_desc                                                                                 |   monto |   perc_total |
#  |---:|:------------------------|:-----------------------------------------------------------------------------------------------|--------:|-------------:|
#  |  0 | Metales y metalmecánica | Fabricación de maquinaria para la explotación de minas y canteras y para obras de construcción |    3117 |        20.48 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'nivel1', 'ciiu_3_4c_desc': 'nivel2', 'perc_total': 'valor'})
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 4 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  21 non-null     object 
#   1   nivel2  21 non-null     object 
#   2   monto   21 non-null     int64  
#   3   valor   21 non-null     float64
#  
#  |    | nivel1                  | nivel2                                                                                         |   monto |   valor |
#  |---:|:------------------------|:-----------------------------------------------------------------------------------------------|--------:|--------:|
#  |  0 | Metales y metalmecánica | Fabricación de maquinaria para la explotación de minas y canteras y para obras de construcción |    3117 |   20.48 |
#  
#  ------------------------------
#  
#  drop_col(col='monto', axis=1)
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  21 non-null     object 
#   1   nivel2  21 non-null     object 
#   2   valor   21 non-null     float64
#  
#  |    | nivel1                  | nivel2                                                                                         |   valor |
#  |---:|:------------------------|:-----------------------------------------------------------------------------------------------|--------:|
#  |  0 | Metales y metalmecánica | Fabricación de maquinaria para la explotación de minas y canteras y para obras de construcción |   20.48 |
#  
#  ------------------------------
#  
#  replace_values(col='nivel2', values={'Fabricación de maquinaria para la explotación de minas y canteras y para obras de construcción': 'Maquinaria para minería, petróleo y construcción', 'Fabricación de sustancias químicas básicas,excepto abonos y compuestos de nitrógeno': 'Sustancias químicas básicas', 'Fabricación de productos de plástico': 'Productos de plástico', 'Fabricación de maquinaria de uso general ncp': 'Maquinarias de uso general', 'Fabricación de bombas, compresores, grifos y válvulas': 'Bombas, compresores y válvulas', 'Fabricación de maquinaria de uso especial ncp': 'Maquinarias de uso especial', 'Fabricación de partes, piezas y accesorios para vehículos automotores y sus motores': 'Partes y piezas para vehículos automotres', 'Fabricación de vehículos automotores': 'Vehículos automotores', 'Fabricación de productos elaborados de metal ncp': 'Otros productos elaborados de metal', 'Fabricación de equipo de elevación y manipulación': 'Equipo de elevación y manipulación', 'Fabricación de cojinetes, engranajes, trenes de engranaje y piezas de transmisión': 'Engranajes y piezas de transmisión', 'Fabricación de productos metálicos para uso estructural y montaje estructural': 'Productos metálicos para uso estructural', 'Fabricación de productos primarios de metales preciosos y metales no ferrosos': 'Productos de metales preciosos y metales no ferrosos', 'Fabricación de cubiertas y cámaras de caucho, recauchutado y renovación de cubiertas de caucho': 'Cubiertas y cámaras de caucho', 'Fabricación de pasta de madera, papel y cartón': 'Pasta de madera, papel y cartón', 'Fabricación de productos de caucho ncp': 'Otros productos de caucho', 'Fabricación de productos químicos ncp': 'Otros productos químicos', 'Industrias básicas de hierro y acero': 'Hierro y acero', 'Fabricación de aparatos de distribución y control de la energía eléctrica': 'Aparatos de control de energía eléctrica', 'Fabricación de artículos de cuchillería, herramientas de mano y artículos de ferretería': 'Artículos de ferretería y herramientas', 'Resto de las actividades': 'Resto de las actividades'})
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   nivel1  21 non-null     object 
#   1   nivel2  21 non-null     object 
#   2   valor   21 non-null     float64
#  
#  |    | nivel1                  | nivel2                                           |   valor |
#  |---:|:------------------------|:-------------------------------------------------|--------:|
#  |  0 | Metales y metalmecánica | Maquinaria para minería, petróleo y construcción |   20.48 |
#  
#  ------------------------------
#  