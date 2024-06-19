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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='iso3 == "ARG"'),
	drop_col(col='iso3', axis=1),
	drop_col(col='location_name_short_en', axis=1),
	drop_col(col='partner_name_short_en', axis=1),
	rename_cols(map={'partner_code': 'geoselector'}),
	wide_to_long(primary_keys=['geoselector'], value_name='valor', var_name='indicador')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 45686 entries, 0 to 45685
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    45686 non-null  object 
#   1   location_name_short_en  45686 non-null  object 
#   2   partner_code            45686 non-null  object 
#   3   partner_name_short_en   45686 non-null  object 
#   4   export_value_pca        9952 non-null   float64
#   5   export_value_pcb        35716 non-null  float64
#  
#  |    | iso3   | location_name_short_en   | partner_code   | partner_name_short_en   |   export_value_pca |   export_value_pcb |
#  |---:|:-------|:-------------------------|:---------------|:------------------------|-------------------:|-------------------:|
#  |  0 | CHE    | Switzerland              | ANT            | Netherlands Antilles    |           0.061569 |                nan |
#  
#  ------------------------------
#  
#  query(condition='iso3 == "ARG"')
#  Index: 296 entries, 630 to 44591
#  Data columns (total 6 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   iso3                    296 non-null    object 
#   1   location_name_short_en  296 non-null    object 
#   2   partner_code            296 non-null    object 
#   3   partner_name_short_en   296 non-null    object 
#   4   export_value_pca        105 non-null    float64
#   5   export_value_pcb        191 non-null    float64
#  
#  |     | iso3   | location_name_short_en   | partner_code   | partner_name_short_en   |   export_value_pca |   export_value_pcb |
#  |----:|:-------|:-------------------------|:---------------|:------------------------|-------------------:|-------------------:|
#  | 630 | ARG    | Argentina                | RWA            | Rwanda                  |                nan |         0.00912515 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3', axis=1)
#  Index: 296 entries, 630 to 44591
#  Data columns (total 5 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   location_name_short_en  296 non-null    object 
#   1   partner_code            296 non-null    object 
#   2   partner_name_short_en   296 non-null    object 
#   3   export_value_pca        105 non-null    float64
#   4   export_value_pcb        191 non-null    float64
#  
#  |     | location_name_short_en   | partner_code   | partner_name_short_en   |   export_value_pca |   export_value_pcb |
#  |----:|:-------------------------|:---------------|:------------------------|-------------------:|-------------------:|
#  | 630 | Argentina                | RWA            | Rwanda                  |                nan |         0.00912515 |
#  
#  ------------------------------
#  
#  drop_col(col='location_name_short_en', axis=1)
#  Index: 296 entries, 630 to 44591
#  Data columns (total 4 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   partner_code           296 non-null    object 
#   1   partner_name_short_en  296 non-null    object 
#   2   export_value_pca       105 non-null    float64
#   3   export_value_pcb       191 non-null    float64
#  
#  |     | partner_code   | partner_name_short_en   |   export_value_pca |   export_value_pcb |
#  |----:|:---------------|:------------------------|-------------------:|-------------------:|
#  | 630 | RWA            | Rwanda                  |                nan |         0.00912515 |
#  
#  ------------------------------
#  
#  drop_col(col='partner_name_short_en', axis=1)
#  Index: 296 entries, 630 to 44591
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   partner_code      296 non-null    object 
#   1   export_value_pca  105 non-null    float64
#   2   export_value_pcb  191 non-null    float64
#  
#  |     | partner_code   |   export_value_pca |   export_value_pcb |
#  |----:|:---------------|-------------------:|-------------------:|
#  | 630 | RWA            |                nan |         0.00912515 |
#  
#  ------------------------------
#  
#  rename_cols(map={'partner_code': 'geoselector'})
#  Index: 296 entries, 630 to 44591
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geoselector       296 non-null    object 
#   1   export_value_pca  105 non-null    float64
#   2   export_value_pcb  191 non-null    float64
#  
#  |     | geoselector   |   export_value_pca |   export_value_pcb |
#  |----:|:--------------|-------------------:|-------------------:|
#  | 630 | RWA           |                nan |         0.00912515 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geoselector'], value_name='valor', var_name='indicador')
#  RangeIndex: 592 entries, 0 to 591
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   geoselector  592 non-null    object 
#   1   indicador    592 non-null    object 
#   2   valor        296 non-null    float64
#  
#  |    | geoselector   | indicador        |   valor |
#  |---:|:--------------|:-----------------|--------:|
#  |  0 | RWA           | export_value_pca |     nan |
#  
#  ------------------------------
#  