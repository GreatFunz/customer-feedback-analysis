# Customer's feedback analysis
Project overview: This project analyzed the feedback of customers on a particular product by tracking customer's comments on real time, analyze and revealed geographical regions, cities  and the customers with concerns.

Steps:

1. Data source: In this project I use  twitter API to track and stream tweets using python. Download World cities Database from https://www.geodatasource.com/download

2. Spark streaming: Stream the twitter data into pyspark, using spark stream reader.

3. Data cleaning and merging: Define data cleaning, merging functions and regular expressions (regex).

4. Model: Define a pipeline using spark ml.

5. Model training: Get training data from http://help.sentiment140.com/for-students , train the model,  use cross validation for model selection and save pipeline model.

6. Prediction: Make a prediction with the trained model

7. Visualization: Data visualization and geopandas for geographical visualization.
