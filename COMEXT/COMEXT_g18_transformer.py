from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='geocodigoFundar == "ARG"'),
	drop_col(col='geocodigoFundar', axis=1),
	drop_col(col='location_name_short_en', axis=1),
	drop_col(col='partner_name_short_en', axis=1),
	drop_col(col='geonombreFundar', axis=1),
	rename_cols(map={'partner_code': 'geocodigo'}),
	wide_to_long(primary_keys=['geocodigo'], value_name='valor', var_name='indicador')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 36868 entries, 0 to 36867
#  Data columns (total 7 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         36868 non-null  object 
#   1   geonombreFundar         36868 non-null  object 
#   2   location_name_short_en  36868 non-null  object 
#   3   partner_code            36868 non-null  object 
#   4   partner_name_short_en   36868 non-null  object 
#   5   export_value_pca        9952 non-null   float64
#   6   export_value_pcb        35716 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | location_name_short_en   | partner_code   | partner_name_short_en   |   export_value_pca |   export_value_pcb |
#  |---:|:------------------|:------------------|:-------------------------|:---------------|:------------------------|-------------------:|-------------------:|
#  |  0 | AFG               | Afganistán        | Afghanistan              | ANT            | Netherlands Antilles    |                  0 |                nan |
#  
#  ------------------------------
#  
#  query(condition='geocodigoFundar == "ARG"')
#  Index: 200 entries, 207 to 11025
#  Data columns (total 7 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         200 non-null    object 
#   1   geonombreFundar         200 non-null    object 
#   2   location_name_short_en  200 non-null    object 
#   3   partner_code            200 non-null    object 
#   4   partner_name_short_en   200 non-null    object 
#   5   export_value_pca        105 non-null    float64
#   6   export_value_pcb        191 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   | location_name_short_en   | partner_code   | partner_name_short_en   |   export_value_pca |   export_value_pcb |
#  |----:|:------------------|:------------------|:-------------------------|:---------------|:------------------------|-------------------:|-------------------:|
#  | 207 | ARG               | Argentina         | Argentina                | AGO            | Angola                  |           0.122579 |          0.0786303 |
#  
#  ------------------------------
#  
#  drop_col(col='geocodigoFundar', axis=1)
#  Index: 200 entries, 207 to 11025
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geonombreFundar         200 non-null    object 
#   1   location_name_short_en  200 non-null    object 
#   2   partner_code            200 non-null    object 
#   3   partner_name_short_en   200 non-null    object 
#   4   export_value_pca        105 non-null    float64
#   5   export_value_pcb        191 non-null    float64
#  
#  |     | geonombreFundar   | location_name_short_en   | partner_code   | partner_name_short_en   |   export_value_pca |   export_value_pcb |
#  |----:|:------------------|:-------------------------|:---------------|:------------------------|-------------------:|-------------------:|
#  | 207 | Argentina         | Argentina                | AGO            | Angola                  |           0.122579 |          0.0786303 |
#  
#  ------------------------------
#  
#  drop_col(col='location_name_short_en', axis=1)
#  Index: 200 entries, 207 to 11025
#  Data columns (total 5 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   geonombreFundar        200 non-null    object 
#   1   partner_code           200 non-null    object 
#   2   partner_name_short_en  200 non-null    object 
#   3   export_value_pca       105 non-null    float64
#   4   export_value_pcb       191 non-null    float64
#  
#  |     | geonombreFundar   | partner_code   | partner_name_short_en   |   export_value_pca |   export_value_pcb |
#  |----:|:------------------|:---------------|:------------------------|-------------------:|-------------------:|
#  | 207 | Argentina         | AGO            | Angola                  |           0.122579 |          0.0786303 |
#  
#  ------------------------------
#  
#  drop_col(col='partner_name_short_en', axis=1)
#  Index: 200 entries, 207 to 11025
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geonombreFundar   200 non-null    object 
#   1   partner_code      200 non-null    object 
#   2   export_value_pca  105 non-null    float64
#   3   export_value_pcb  191 non-null    float64
#  
#  |     | geonombreFundar   | partner_code   |   export_value_pca |   export_value_pcb |
#  |----:|:------------------|:---------------|-------------------:|-------------------:|
#  | 207 | Argentina         | AGO            |           0.122579 |          0.0786303 |
#  
#  ------------------------------
#  
#  drop_col(col='geonombreFundar', axis=1)
#  Index: 200 entries, 207 to 11025
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   partner_code      200 non-null    object 
#   1   export_value_pca  105 non-null    float64
#   2   export_value_pcb  191 non-null    float64
#  
#  |     | partner_code   |   export_value_pca |   export_value_pcb |
#  |----:|:---------------|-------------------:|-------------------:|
#  | 207 | AGO            |           0.122579 |          0.0786303 |
#  
#  ------------------------------
#  
#  rename_cols(map={'partner_code': 'geocodigo'})
#  Index: 200 entries, 207 to 11025
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         200 non-null    object 
#   1   export_value_pca  105 non-null    float64
#   2   export_value_pcb  191 non-null    float64
#  
#  |     | geocodigo   |   export_value_pca |   export_value_pcb |
#  |----:|:------------|-------------------:|-------------------:|
#  | 207 | AGO         |           0.122579 |          0.0786303 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geocodigo'], value_name='valor', var_name='indicador')
#  RangeIndex: 400 entries, 0 to 399
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  400 non-null    object 
#   1   indicador  400 non-null    object 
#   2   valor      296 non-null    float64
#  
#  |    | geocodigo   | indicador        |    valor |
#  |---:|:------------|:-----------------|---------:|
#  |  0 | AGO         | export_value_pca | 0.122579 |
#  
#  ------------------------------
#  