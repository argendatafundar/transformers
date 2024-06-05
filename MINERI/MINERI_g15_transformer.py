from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

@transformer.convert
def fecha_to_trimestre(df, col, trimestre_map, input_format_pattern='(.*)_([0-9]{4})', extractor=['trimester', 'year'], trimestre_format='{year}-{trimester}-1'):
    from re import findall

    def get_trimestre(fecha):
        groups = findall(input_format_pattern, fecha)[0]
        trimester_index = extractor.index('trimester')
        year_index = extractor.index('year')
        trimester = groups[trimester_index]
        year = groups[year_index]
        return trimestre_format.format(year=year, trimester=trimestre_map[trimester])
    
    df[col] = df[col].map(get_trimestre)

    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador'),
	fecha_to_trimestre(col='fecha', trimestre_map={'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'VI': 6}, input_format_pattern='(.*)_([0-9]{4})', extractor=['trimester', 'year'], trimestre_format='{year}-{trimester}-1')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 107 entries, 0 to 106
#  Data columns (total 2 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   fecha              107 non-null    object 
#   1   tasa_feminizacion  107 non-null    float64
#  
#  |    | fecha   |   tasa_feminizacion |
#  |---:|:--------|--------------------:|
#  |  0 | I_1996  |             6.62948 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['fecha'], value_name='valor', var_name='indicador')
#  RangeIndex: 107 entries, 0 to 106
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   fecha      107 non-null    object 
#   1   indicador  107 non-null    object 
#   2   valor      107 non-null    float64
#  
#  |    | fecha    | indicador         |   valor |
#  |---:|:---------|:------------------|--------:|
#  |  0 | 1996-1-1 | tasa_feminizacion | 6.62948 |
#  
#  ------------------------------
#  
#  fecha_to_trimestre(col='fecha', trimestre_map={'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'VI': 6}, input_format_pattern='(.*)_([0-9]{4})', extractor=['trimester', 'year'], trimestre_format='{year}-{trimester}-1')
#  RangeIndex: 107 entries, 0 to 106
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   fecha      107 non-null    object 
#   1   indicador  107 non-null    object 
#   2   valor      107 non-null    float64
#  
#  |    | fecha    | indicador         |   valor |
#  |---:|:---------|:------------------|--------:|
#  |  0 | 1996-1-1 | tasa_feminizacion | 6.62948 |
#  
#  ------------------------------
#  