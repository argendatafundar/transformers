from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='geocodigoFundar == "ARG"'),
	rename_cols(map={'geocodigoFundar': 'geocodigo', 'sector_bp_name': 'categoria', 'export_value_pc': 'valor'}),
	drop_col(col='country_name_abbreviation', axis=1),
	drop_col(col='sector_bp', axis=1),
	replace_value(col='categoria', curr_value='Cueros y pieles', new_value='Cueros y pieles'),
	replace_value(col='categoria', curr_value='Metales', new_value='Metales'),
	replace_value(col='categoria', curr_value='Plásticos y gomas', new_value='Plásticos y gomas'),
	replace_value(col='categoria', curr_value='Químicos', new_value='Químicos'),
	replace_value(col='categoria', curr_value='Textiles, indumentaria y calzado', new_value='Textiles, indumentaria y calzado'),
	replace_value(col='categoria', curr_value='Productos agropecuarios', new_value='Productos agropecuarios'),
	replace_value(col='categoria', curr_value='Alimentos procesados', new_value='Alimentos procesados'),
	replace_value(col='categoria', curr_value='Madera', new_value='Madera'),
	replace_value(col='categoria', curr_value='Piedra y vidrio', new_value='Piedra y vidrio'),
	replace_value(col='categoria', curr_value='Transporte', new_value='Transporte'),
	replace_value(col='categoria', curr_value='Maquinaria', new_value='Maquinaria'),
	replace_value(col='categoria', curr_value='Otros productos industriales', new_value='Otros productos industriales'),
	replace_value(col='categoria', curr_value='Minerales', new_value='Minerales')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 2930 entries, 0 to 2929
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   geocodigoFundar            2930 non-null   object 
#   1   geonombreFundar            2930 non-null   object 
#   2   anio                       2930 non-null   int64  
#   3   country_name_abbreviation  2930 non-null   object 
#   4   sector_bp                  2930 non-null   int64  
#   5   sector_bp_name             2930 non-null   object 
#   6   export_value_pc            2930 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | country_name_abbreviation   |   sector_bp | sector_bp_name       |   export_value_pc |
#  |---:|:------------------|:------------------|-------:|:----------------------------|------------:|:---------------------|------------------:|
#  |  0 | BGD               | Bangladesh        |   2020 | Bangladesh                  |           2 | Alimentos procesados |          0.726726 |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar == "ARG"')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   geocodigoFundar            13 non-null     object 
#   1   geonombreFundar            13 non-null     object 
#   2   anio                       13 non-null     int64  
#   3   country_name_abbreviation  13 non-null     object 
#   4   sector_bp                  13 non-null     int64  
#   5   sector_bp_name             13 non-null     object 
#   6   export_value_pc            13 non-null     float64
#  
#  |     | geocodigoFundar   | geonombreFundar   |   anio | country_name_abbreviation   |   sector_bp | sector_bp_name   |   export_value_pc |
#  |----:|:------------------|:------------------|-------:|:----------------------------|------------:|:-----------------|------------------:|
#  | 477 | ARG               | Argentina         |   2020 | Argentina                   |           6 | Cueros y pieles  |           0.97719 |
#  
#  ------------------------------
#  
#  rename_cols(map={'geocodigoFundar': 'geocodigo', 'sector_bp_name': 'categoria', 'export_value_pc': 'valor'})
#  Index: 13 entries, 477 to 2607
#  Data columns (total 7 columns):
#   #   Column                     Non-Null Count  Dtype  
#  ---  ------                     --------------  -----  
#   0   geocodigo                  13 non-null     object 
#   1   geonombreFundar            13 non-null     object 
#   2   anio                       13 non-null     int64  
#   3   country_name_abbreviation  13 non-null     object 
#   4   sector_bp                  13 non-null     int64  
#   5   categoria                  13 non-null     object 
#   6   valor                      13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | country_name_abbreviation   |   sector_bp | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------------------|------------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Argentina                   |           6 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  drop_col(col='country_name_abbreviation', axis=1)
#  Index: 13 entries, 477 to 2607
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   sector_bp        13 non-null     int64  
#   4   categoria        13 non-null     object 
#   5   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio |   sector_bp | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|------------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 |           6 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  drop_col(col='sector_bp', axis=1)
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Cueros y pieles', new_value='Cueros y pieles')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Metales', new_value='Metales')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Plásticos y gomas', new_value='Plásticos y gomas')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Químicos', new_value='Químicos')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Textiles, indumentaria y calzado', new_value='Textiles, indumentaria y calzado')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Productos agropecuarios', new_value='Productos agropecuarios')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Alimentos procesados', new_value='Alimentos procesados')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Madera', new_value='Madera')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Piedra y vidrio', new_value='Piedra y vidrio')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Transporte', new_value='Transporte')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Maquinaria', new_value='Maquinaria')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Otros productos industriales', new_value='Otros productos industriales')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Minerales', new_value='Minerales')
#  Index: 13 entries, 477 to 2607
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigo        13 non-null     object 
#   1   geonombreFundar  13 non-null     object 
#   2   anio             13 non-null     int64  
#   3   categoria        13 non-null     object 
#   4   valor            13 non-null     float64
#  
#  |     | geocodigo   | geonombreFundar   |   anio | categoria       |   valor |
#  |----:|:------------|:------------------|-------:|:----------------|--------:|
#  | 477 | ARG         | Argentina         |   2020 | Cueros y pieles | 0.97719 |
#  
#  ------------------------------
#  