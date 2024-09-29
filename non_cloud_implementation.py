import time
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# Step 1: Set up SparkSession
start_time = time.time()
spark = SparkSession.builder.appName("Local ETL Pipeline with Visualization").getOrCreate()

# Step 2: Load data 
input_file = "/Users/richie/Downloads/sales_data.csv"
df = spark.read.csv(input_file, header=True, inferSchema=True)

# Step 3: Perform transformations
category_grouped_df = df.groupBy("Product_Category").count()

# Step 4: Collect the result to a Pandas DataFrame for visualization
category_data = category_grouped_df.toPandas()

# Step 5: Visualize the data using matplotlib
plt.figure(figsize=(8, 8))

# Prepare the data for the pie chart
labels = category_data['Product_Category']
sizes = category_data['count']

# Create a pie chart
plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
plt.title("Order Distribution by Product Category")
plt.axis('equal')  
plt.tight_layout()

# Save the pie chart to a file
plt.savefig("/Users/richie/Downloads/product_category_piechart.png")

# Save the transformed data to a local file
output_file = "/Users/richie/Downloads/sales_data_with_visualization.csv"
category_grouped_df.write.csv(output_file, header=True, mode="overwrite")

# Step 6: Stop the SparkSession
spark.stop()
end_time = time.time()

print(f"Local pipeline execution time: {end_time - start_time} seconds")
