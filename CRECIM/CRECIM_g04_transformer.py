from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio == anio.max()'),
	drop_col(col='anio', axis=1),
	drop_col(col='geocodigoFundar', axis=1),
	drop_col(col='medida_bienestar', axis=1),
	drop_col(col='reporting_level', axis=1),
	drop_col(col='poverty_line', axis=1),
	wide_to_long(primary_keys=['geonombreFundar', 'continente_fundar'], value_name='valor', var_name='indicador')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 5651 entries, 0 to 5650
#  Data columns (total 9 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    5651 non-null   object 
#   1   geonombreFundar    5651 non-null   object 
#   2   continente_fundar  5651 non-null   object 
#   3   anio               5651 non-null   int64  
#   4   medida_bienestar   5651 non-null   object 
#   5   reporting_level    5651 non-null   object 
#   6   poverty_line       5651 non-null   float64
#   7   promedio           5651 non-null   float64
#   8   pib_pc             5651 non-null   float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   anio | medida_bienestar   | reporting_level   |   poverty_line |   promedio |   pib_pc |
#  |---:|:------------------|:------------------|:--------------------|-------:|:-------------------|:------------------|---------------:|-----------:|---------:|
#  |  0 | AGO               | Angola            | África              |   1990 | consumo            | national          |           2.15 |     9.5539 |  7391.75 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 164 entries, 33 to 5650
#  Data columns (total 9 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    164 non-null    object 
#   1   geonombreFundar    164 non-null    object 
#   2   continente_fundar  164 non-null    object 
#   3   anio               164 non-null    int64  
#   4   medida_bienestar   164 non-null    object 
#   5   reporting_level    164 non-null    object 
#   6   poverty_line       164 non-null    float64
#   7   promedio           164 non-null    float64
#   8   pib_pc             164 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   |   anio | medida_bienestar   | reporting_level   |   poverty_line |   promedio |   pib_pc |
#  |---:|:------------------|:------------------|:--------------------|-------:|:-------------------|:------------------|---------------:|-----------:|---------:|
#  | 33 | AGO               | Angola            | África              |   2023 | consumo            | national          |           2.15 |     5.5384 |  7244.89 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  Index: 164 entries, 33 to 5650
#  Data columns (total 8 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    164 non-null    object 
#   1   geonombreFundar    164 non-null    object 
#   2   continente_fundar  164 non-null    object 
#   3   medida_bienestar   164 non-null    object 
#   4   reporting_level    164 non-null    object 
#   5   poverty_line       164 non-null    float64
#   6   promedio           164 non-null    float64
#   7   pib_pc             164 non-null    float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | continente_fundar   | medida_bienestar   | reporting_level   |   poverty_line |   promedio |   pib_pc |
#  |---:|:------------------|:------------------|:--------------------|:-------------------|:------------------|---------------:|-----------:|---------:|
#  | 33 | AGO               | Angola            | África              | consumo            | national          |           2.15 |     5.5384 |  7244.89 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 164 entries, 33 to 5650
#  Data columns (total 7 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    164 non-null    object 
#   1   continente_fundar  164 non-null    object 
#   2   medida_bienestar   164 non-null    object 
#   3   reporting_level    164 non-null    object 
#   4   poverty_line       164 non-null    float64
#   5   promedio           164 non-null    float64
#   6   pib_pc             164 non-null    float64
#  
#  |    | geonombreFundar   | continente_fundar   | medida_bienestar   | reporting_level   |   poverty_line |   promedio |   pib_pc |
#  |---:|:------------------|:--------------------|:-------------------|:------------------|---------------:|-----------:|---------:|
#  | 33 | Angola            | África              | consumo            | national          |           2.15 |     5.5384 |  7244.89 |
#  
#  ------------------------------
#  
#  drop_col(col='medida_bienestar', axis=1)
#  Index: 164 entries, 33 to 5650
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    164 non-null    object 
#   1   continente_fundar  164 non-null    object 
#   2   reporting_level    164 non-null    object 
#   3   poverty_line       164 non-null    float64
#   4   promedio           164 non-null    float64
#   5   pib_pc             164 non-null    float64
#  
#  |    | geonombreFundar   | continente_fundar   | reporting_level   |   poverty_line |   promedio |   pib_pc |
#  |---:|:------------------|:--------------------|:------------------|---------------:|-----------:|---------:|
#  | 33 | Angola            | África              | national          |           2.15 |     5.5384 |  7244.89 |
#  
#  ------------------------------
#  
#  drop_col(col='reporting_level', axis=1)
#  Index: 164 entries, 33 to 5650
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    164 non-null    object 
#   1   continente_fundar  164 non-null    object 
#   2   poverty_line       164 non-null    float64
#   3   promedio           164 non-null    float64
#   4   pib_pc             164 non-null    float64
#  
#  |    | geonombreFundar   | continente_fundar   |   poverty_line |   promedio |   pib_pc |
#  |---:|:------------------|:--------------------|---------------:|-----------:|---------:|
#  | 33 | Angola            | África              |           2.15 |     5.5384 |  7244.89 |
#  
#  ------------------------------
#  
#  drop_col(col='poverty_line', axis=1)
#  Index: 164 entries, 33 to 5650
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    164 non-null    object 
#   1   continente_fundar  164 non-null    object 
#   2   promedio           164 non-null    float64
#   3   pib_pc             164 non-null    float64
#  
#  |    | geonombreFundar   | continente_fundar   |   promedio |   pib_pc |
#  |---:|:------------------|:--------------------|-----------:|---------:|
#  | 33 | Angola            | África              |     5.5384 |  7244.89 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geonombreFundar', 'continente_fundar'], value_name='valor', var_name='indicador')
#  RangeIndex: 328 entries, 0 to 327
#  Data columns (total 4 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geonombreFundar    328 non-null    object 
#   1   continente_fundar  328 non-null    object 
#   2   indicador          328 non-null    object 
#   3   valor              328 non-null    float64
#  
#  |    | geonombreFundar   | continente_fundar   | indicador   |   valor |
#  |---:|:------------------|:--------------------|:------------|--------:|
#  |  0 | Angola            | África              | promedio    |  5.5384 |
#  
#  ------------------------------
#  