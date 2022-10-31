**Data Modeling – SQL DB**

**Purpose of the project**

The goal of this project is to wrangle WeRateDogs Twitter data to create interesting and trustworthy analyses and visualizations.

**Content**
The project will consist of the following parts:

1. Data Gathering
The data is gathered from three sources: a given csv file, a tsv file from udacity servers and from twitter API.

2. Data assessment
All data is assessed visually and programmatically for quality and tidiness issues.

3. Data cleaning
All the issues documented while assessing are cleaned.

4. Data Storage
The data then is stored in a main CSV file.

5. Data Analysis and Visualization
Data analysis and visualizations are performed to cature the valuable insigths.

6. Reporting
Finally, two reports are created. The first one describes the wrangling efforts and the second that displays visualizations and comment on the insights discovered.

**Files description**

- .gitignore - prevents files to be added to Git/Github.
- act_report.ipynb - communicates all the insights and displays the visualizations produced from wrangled data.
- act_report.pdf - pdf version of act_report.
- barplot.png, scatterplot.png, piechart.png - data visualizations of insights.
- data_gathering_api.ipynb - jupyter notebook that scrapes the data from Twitter API.
- image_predictions.tsv - file gather from Udacity servers containing data of neural network predictions of dog breeds.
- tweet_json.txt - data of twitts' retweet and favourite count gathered from Twitter API - Tweepy.
- twitter_archive_master.csv - main datafile that contains all the cleaned data.  
- twitter-archive-enhanced.csv - data of tweets with hastag @dog_rates provided by Udacity. 
- wrangle_act.ipynb - main logic of the project, contains all the steps from 'Content' section.
- wrangle_report.ipynb - describes wrangling efforts.
- wrangle_report.pdf - pdf version of wrange_report.
