import pycountry_convert as pcc
from plotly.express import choropleth
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import round
import plotly.express as px

# name spark session
spark = SparkSession.builder.appName('End to end processing').getOrCreate()

# create dataframe from our csv file in input
df = spark.read.csv('input/global_income.csv' , header = True, inferSchema = True)

# standardize or clean columns
new_column_names = [col_name.replace(' ','_')
                    .replace('(','')
                    .replace(')','')
                    for col_name in df.columns]

# create and replace columns names in dataframe
df = df.toDF(*new_column_names)

# drop all null columns
df = df.dropna(how='all')

# select columns and order
df = df.select('Year','Country','Population' , 'Gini_Index' , 'Average_Income_USD')

# function to get the continent name
def get_continent_name(country_name):
    try:
        country_code = pcc.country_name_to_country_alpha2(country_name, cn_name_format='default')
        continent_code = pcc.country_alpha2_to_continent_code(country_code)
        return pcc.convert_continent_code_to_continent_name(continent_code)
    except:
        return None

# round Gini_Index and Average_Income_[USD] columns to 2 decimals and . to ,
df = df.withColumn('Gini_Index', round(df['Gini_Index'], 2)) \
       .withColumn('Average_Income_USD', round(df['Average_Income_USD'], 2)) \
       .withColumn('Population', round(df['Population'], 0))


# create the Continent column by calling the function
continent_udf = udf(get_continent_name, StringType())

# insert the Continent column in dataframe
df = df.withColumn('Continent', continent_udf(df['Country']))

# reorder the dataframe columns
df = df.select('Continent','Country','Year', 'Population' , 'Gini_Index' , 'Average_Income_USD')

df.createGlobalTempView('global_income')

# Average income by continent visualisation - 1
df_cont = spark.sql("""
    SELECT 
        Continent, 
        Year, 
        AVG(Average_Income_USD) AS Avg_Income
    FROM global_temp.global_income    
    WHERE Continent IS NOT NULL
    GROUP BY Continent, Year          
""")

# convert to Pandas
df_cont = df_cont.toPandas()

# setting the columns to vectors and data display
fig = px.bar(df_cont, x='Year', y='Avg_Income', color = 'Continent', barmode = 'group')

# update the names
fig.update_layout( title_text ="Number of Average Income in Continents by Year",
                   xaxis_title ='Year',
                   yaxis_title ='Average Income',
                   legend_title ='Continent')

# write to a html file
fig.write_html('output/average_income_by_year_continent.html')

# Top 10 most unequal country in the years of 2021 to 2023 visualisation - 2
df_country = spark.sql("""
    SELECT 
        Year, 
        Country, 
        Index
    FROM (
        SELECT 
            Year, 
            Country, 
            Gini_Index AS Index, 
            ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Gini_Index DESC) as rank
        FROM global_temp.global_income 
        WHERE Country IS NOT NULL
            AND Country NOT IN ('total', 'others')
    ) ranked
    WHERE rank <= 10
        AND Year >= 2021
    ORDER BY Year, Index DESC;
""")

# convert to Pandas
df_country= df_country.toPandas()

# setting the columns to vectors and data display
fig = px.bar(df_country, x='Year', y='Index', color = 'Country', barmode = 'group')

# update the names
fig.update_layout( title_text ="Top 10 unequal country by year",
                   xaxis_title ='Year',
                   yaxis_title ='Gini Index',
                   legend_title ='Country')

# write to a html file
fig.write_html('output/top_10_unequal_by_year.html')

# display the output on map visualisation - 3
df_country_year_map = spark.sql("""
    SELECT 
        Year, 
        Country, 
        Gini_Index AS Index
    FROM (
        SELECT 
            Year, 
            Country, 
            Gini_Index,
            ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Gini_Index DESC) AS rank
        FROM global_temp.global_income
        WHERE Country IS NOT NULL
) ranked
WHERE rank <= 10
ORDER BY Year ASC;
""")

# convert to Pandas
df_country_year_map = df_country_year_map.toPandas()

# setting the columns to vectors and data display
fig = choropleth(df_country_year_map, locations = 'Country',
                 color = 'Index',
                 hover_name= 'Country',
                 animation_frame = 'Year',
                 range_color = [100000 ,100000],
                 color_continuous_scale = px.colors.sequential.Plasma,
                 locationmode = 'country names',
                 title = 'Yearly unequal by country')

# write to a html file
fig.write_html('output/unequal_country_map_by_year.html')

# write into csv file
df.write.csv("output/global_income_transformed.csv", header = True, mode = 'overwrite')

spark.stop()