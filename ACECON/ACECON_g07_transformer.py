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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'sector': 'indicador'}),
	replace_values(col='indicador', values={'agricultura_ganaderia_caza_y_silvicultura': 'Agricultura, caza y silvicultura', 'pesca': 'Pesca', 'explotacion_de_minas_y_canteras': 'Explotación de minas y canteras', 'industria_manufacturera': 'Industrias manufactureras', 'electricidad_gas_y_agua': 'Electricidad, gas y agua', 'construccion': 'Construcción', 'comercio_mayorista_minorista_hoteles_restaurantes': 'Comercio al por mayor y menor, y hoteles y restaurantes', 'transporte_almacenamiento_y_comunicaciones': 'Transporte, almacenamiento y comunicaciones', 'intermediacion_financiera': 'Intermediación financiera', 'actividades_inmobiliarias_empresariales_y_de_alquiler': 'Actividades inmobiliarias, empresariales y de alquiler', 'administracion_publica_y_defensa_planes_de_seguridad_social_de_afiliacion_obligatoria': 'Admin. pública, defensa y org extraterr.', 'otros_servicios': 'Otros servicios'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1068 entries, 0 to 1067
#  Data columns (total 3 columns):
#   #   Column  Non-Null Count  Dtype  
#  ---  ------  --------------  -----  
#   0   anio    1068 non-null   int64  
#   1   sector  1068 non-null   object 
#   2   valor   1068 non-null   float64
#  
#  |    |   anio | sector                                    |   valor |
#  |---:|-------:|:------------------------------------------|--------:|
#  |  0 |   1935 | agricultura_ganaderia_caza_y_silvicultura | 25.7423 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador'})
#  RangeIndex: 1068 entries, 0 to 1067
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1068 non-null   int64  
#   1   indicador  1068 non-null   object 
#   2   valor      1068 non-null   float64
#  
#  |    |   anio | indicador                                 |   valor |
#  |---:|-------:|:------------------------------------------|--------:|
#  |  0 |   1935 | agricultura_ganaderia_caza_y_silvicultura | 25.7423 |
#  
#  ------------------------------
#  
#  replace_values(col='indicador', values={'agricultura_ganaderia_caza_y_silvicultura': 'Agricultura, caza y silvicultura', 'pesca': 'Pesca', 'explotacion_de_minas_y_canteras': 'Explotación de minas y canteras', 'industria_manufacturera': 'Industrias manufactureras', 'electricidad_gas_y_agua': 'Electricidad, gas y agua', 'construccion': 'Construcción', 'comercio_mayorista_minorista_hoteles_restaurantes': 'Comercio al por mayor y menor, y hoteles y restaurantes', 'transporte_almacenamiento_y_comunicaciones': 'Transporte, almacenamiento y comunicaciones', 'intermediacion_financiera': 'Intermediación financiera', 'actividades_inmobiliarias_empresariales_y_de_alquiler': 'Actividades inmobiliarias, empresariales y de alquiler', 'administracion_publica_y_defensa_planes_de_seguridad_social_de_afiliacion_obligatoria': 'Admin. pública, defensa y org extraterr.', 'otros_servicios': 'Otros servicios'})
#  RangeIndex: 1068 entries, 0 to 1067
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       1068 non-null   int64  
#   1   indicador  1068 non-null   object 
#   2   valor      1068 non-null   float64
#  
#  |    |   anio | indicador                        |   valor |
#  |---:|-------:|:---------------------------------|--------:|
#  |  0 |   1935 | Agricultura, caza y silvicultura | 25.7423 |
#  
#  ------------------------------
#  