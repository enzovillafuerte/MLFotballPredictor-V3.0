import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import numpy as np
import os
import warnings
import time
from scipy.stats import poisson
from datetime import datetime
import logging


########################################################################################################
################################### SCRAPER SECTION ####################################################
########################################################################################################

"""
In this section we define the function we will use on base urls to scrape data from FBREf. There are multiple tables in the HTML code:

Row indexes:                                                  | Naming Convetion 
    - [2] -> Squad Standard Stats, Squad Stats                | '_Standard'
    - [4] -> Squad Goalkeeping, Squad Stats                   | '_GK'
    - [6] -> Squad Advanced Goalkeeping, Squad Stats          | '_AdvGK'
    - [8] -> Squad Shooting, Squad Stats                      | '_Shooting'
    - [10] -> Squad Passing, Squad Stats                      | '_Passing'
    - [12] -> Squad Pass Types, Squad Stats                   | '_PassTypes'
    - [14] -> Squad Goal and Shot Creation, Squad Stats       | '_G&SCreation'
    - [16] -> Squad Defensive Actions, Squad Stats            | '_DefActions'
    - [18] -> Squad Possession, Squad Stats                   | '_Possession'
    - [20] -> Squad Playing Time, Squad Stats                 | '_PlayTime'
    - [22] -> Squad Misc Stats, Squad Stats                   | '_Misc'
"""
# **********************************************************************
# ****** Scraping the Overall Stats for each Team ************
# **********************************************************************

def scrape_fbref_xG(url):
    
    # Creatind a dictionary of index and labels. See comment up there
    
    dict_index = { 
        2: '_Standard',
        4: '_GK',
        6: '_AdvGK',
        8: '_Shooting',
        10: '_Passing',
        12: '_PassTypes',
        14: 'G&SCreation',
        16: '_DefActions',
        18: '_Possession',
        20: '_PlayTime',
        22: '_Misc'
    }
    
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    
    # Initializing an empty dataframe
    merged_df = None
    
    for index, label in dict_index.items():
        
        # Extract the specific table (assuming it's the 3rd table on the page)
        tables = data.find_all('table')
        rows = tables[index].find_all('tr')

        # Extract the headers
        headers = rows[1].find_all('th')
        column_titles = [header.get_text(strip=True) for header in headers]

        # Creating title label for accompanying the default naming convention
        ## IF statements here
        title_label = label

        # Add '_Standard' to each column name
        column_titles = [column_titles[0]] + [title + title_label for title in column_titles[1:]]

        # Check the number of columns expected
        print(f"Expected number of columns: {len(column_titles)}")

        # Extract the data rows
        table_data = []
        for row in rows[1:]:  # Start from the second row to skip the header row
            cols_a = [col.get_text(strip=True) for col in row.find_all('a')]
            cols_b = [col.get_text(strip=True) for col in row.find_all('td')]
            combined_cols = cols_a + cols_b

            # Print the length of combined columns for debugging
            # print(f"Row has {len(combined_cols)} columns: {combined_cols}")

            # Append only if the number of columns matches the headers
            # The first row was empty, that's why the error in the previous block of code. Keep this one
            if len(combined_cols) == len(column_titles):
                table_data.append(combined_cols)
            else:
                print(f"Skipping row with {len(combined_cols)} columns, expected {len(column_titles)}.")
    
        # Create DataFrame and set column titles
        df = pd.DataFrame(table_data, columns=column_titles)
        
        # Merge dataframe with the existing dataframe
        if merged_df is None:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, how='outer', on='Squad')
            
    # Adding an extraction date column
    today = datetime.today()
    merged_df['Extraction Date'] = today
    
    return merged_df

# Example usage
url = 'https://fbref.com/en/comps/9/Premier-League-Stats'
df = scrape_fbref_xG(url)

print(df)


# Creating a dictionary of urls with leagues with xG data available
fbref_urls = {
    'EPL': 'https://fbref.com/en/comps/9/Premier-League-Stats',
    'La Liga': 'https://fbref.com/en/comps/12/La-Liga-Stats',
    'Bundesliga': 'https://fbref.com/en/comps/20/Bundesliga-Stats',
    'Serie A': 'https://fbref.com/en/comps/11/Serie-A-Stats',
    'Ligue 1': 'https://fbref.com/en/comps/13/Ligue-1-Stats'} #,
    #'Eredivisie': 'https://fbref.com/en/comps/23/2023-2024/2023-2024-Eredivisie-Stats',
    #'Bundesliga_2': 'https://fbref.com/en/comps/33/2-Bundesliga-Stats'}#,
    #'Jupiler': 'https://fbref.com/en/comps/37/Belgian-Pro-League-Stats', - Different Format
    #'Liga MX': 'https://fbref.com/en/comps/31/Liga-MX-Stats',
    #'Primeira Liga': 'https://fbref.com/en/comps/32/Primeira-Liga-Stats',
    #'Liga Argentina': 'https://fbref.com/en/comps/21/Primera-Division-Stats',
    #'Brasileirao': 'https://fbref.com/en/comps/24/Serie-A-Stats'} #,
    # 'MLS': 'https://fbref.com/en/comps/22/Major-League-Soccer-Stats' } - Different Format (Would be great to have MLS there), consider using try excepts like in V2 

# Creating a dictionary to store the scraped data for each league
standings = {}

for league, url in fbref_urls.items():
    
    try:
        # global variable here
        standings[league] = scrape_fbref_xG(url)
        time.sleep(2) # To avoid problems at making to many requests to fbref
        
    except Exception as e:
        print(f"Failed to scrape standings data for {league}")

# **********************************************************************
# ****** Scraping Future Fixtures and performing the merge ************
# **********************************************************************

# Not technically scraping as we use pandas but is fine
# --
def fbref_fixtures(url):
    
    # Using pandas to read the table
    fixtures = pd.read_html(url)

    # We are interested in the first table
    fixtures_df = fixtures[0]

    # Only wanna keep future matches
    fixtures_df = fixtures_df[fixtures_df['Match Report'] == 'Head-to-Head']
    fixtures_df.columns
    
    # Keeping only needed columns
    fixtures_df = fixtures_df[['Home', 'Away', 'Date']]

    # One column for Match (Team 1 vs Team 2)
    fixtures_df['Match'] = fixtures_df['Home'] +' ' + 'Vs' + ' ' +  fixtures_df['Away']

    # We want to keep only the top 10 games (9 in germany and other leagues, but leave it like that for simplicity)
    fixtures_df = fixtures_df.head(10)

    return fixtures_df


# Defining a function to extract pattern from standings url and transform url into fixtures url
def get_fixtures_url(standings_url):
    base_url = standings_url.rsplit('/', 1)[0]
    competition_id = standings_url.split('/')[-2]
    return f"{base_url}/schedule/{competition_id}-Scores-and-Fixtures"


# Defining new dictionary
fixtures_url = {}

# Getting new urls for FBRef leagues with xG data
for country, url in fbref_urls.items():
    fixtures_url[country] = get_fixtures_url(url)

# Define another variable to stored scraped data 
# Standings data is in standings dictionary
# Fixtures data is in fixtures dictionary
fixtures = {}

# Scraping the fixture data 
for league, url in fixtures_url.items():

    try:
        fixtures[league] = fbref_fixtures(url)
    
    except Exception as e:
        print(f"Failed to scrape fixtures data for {league}")


# **********************************************************************
# ****************** MERGE ********************
# **********************************************************************

merged_df = {}

for league, fixture_data in fixtures.items():
    if league in standings:
        # Merge Home team standings
        home_merge = pd.merge(fixture_data, standings[league], left_on='Home', right_on='Squad', suffixes=('', '_Home'))
        # Merge Away team standings
        merged_df[league] = pd.merge(home_merge, standings[league], left_on='Away', right_on='Squad', suffixes=('', '_Away'))
    else:
        print(f"Missing standings data for {league}, skipping merge.")


# **********************************************************************
# ****************** SAVE INTO SPECIFIC FORMAT ********************
# **********************************************************************

# Json
today_date = datetime.today().strftime('%Y%m%d')
filename = f'Data/merged_df_{today_date}.json'

# Convert DataFrames to dictionaries and handle Timestamps
merged_df_serializable = {
    league: df.applymap(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x).to_dict(orient='records')
    for league, df in merged_df.items()
}

# Save to JSON file
with open(filename, 'w') as json_file:
    json.dump(merged_df_serializable, json_file, indent=4)

print(f"File saved as {filename}")

# python main.py