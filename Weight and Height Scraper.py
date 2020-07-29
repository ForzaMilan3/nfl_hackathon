import boto3
from bs4 import BeautifulSoup, Comment
import io
import numpy as np
import os
import pandas as pd
import re
import requests
from sagemaker import get_execution_role

table_url = 'https://www.pro-football-reference.com/play-index/psl_finder.cgi?request=1&match=single&year_min=2017&year_max=2019&season_start=1&season_end=-1&pos%5B%5D=qb&pos%5B%5D=rb&pos%5B%5D=wr&pos%5B%5D=te&pos%5B%5D=e&pos%5B%5D=t&pos%5B%5D=g&pos%5B%5D=c&pos%5B%5D=ol&pos%5B%5D=dt&pos%5B%5D=de&pos%5B%5D=dl&pos%5B%5D=ilb&pos%5B%5D=olb&pos%5B%5D=lb&pos%5B%5D=cb&pos%5B%5D=s&pos%5B%5D=db&pos%5B%5D=k&pos%5B%5D=p&draft_year_min=1936&draft_year_max=2020&draft_pick_in_round=pick_overall&conference=any&draft_pos%5B%5D=qb&draft_pos%5B%5D=rb&draft_pos%5B%5D=wr&draft_pos%5B%5D=te&draft_pos%5B%5D=e&draft_pos%5B%5D=t&draft_pos%5B%5D=g&draft_pos%5B%5D=c&draft_pos%5B%5D=ol&draft_pos%5B%5D=dt&draft_pos%5B%5D=de&draft_pos%5B%5D=dl&draft_pos%5B%5D=ilb&draft_pos%5B%5D=olb&draft_pos%5B%5D=lb&draft_pos%5B%5D=cb&draft_pos%5B%5D=s&draft_pos%5B%5D=db&draft_pos%5B%5D=k&draft_pos%5B%5D=p&c5val=1.0&c6stat=weight&order_by=player&order_by_asc=Y&offset={0}'
list_number = np.arange(0,6100,100)

new_df = pd.DataFrame(columns=['first_name', 'last_name', 'year', 'team', 'height', 'weight'])

for x in list_number:
    page = requests.get(table_url.format(str(x)))
    soup = BeautifulSoup(page.content, 'html.parser')

    table = soup.find('table', {'id': 'results'}).find('tbody').find_all('tr')
    for item in table:
        if item.has_attr('class') and item['class'][0] == 'thead':
            pass
        else:
            first_team_data = []
            second_team_data = []
            third_team_data = []
            first_name = item.find('td', {'data-stat':'player'}).get('csk').split(',')[1].strip()
            last_name = item.find('td', {'data-stat':'player'}).get('csk').split(',')[0].strip()
            height = int(item.find('td', {'data-stat':'height_in'}).get_text().split('-')[0])*12+int(item.find('td', {'data-stat':'height_in'}).get_text().split('-')[1])
            weight = item.find('td', {'data-stat':'weight'}).get_text()
            if item.find('td', {'data-stat':'team'}).get_text() == '2TM':
                year = item.find('td', {'data-stat':'year_id'}).get_text()
                player_link = item.find('td', {'data-stat':'player'}).find('a').get('href')
                player_page = requests.get('https://www.pro-football-reference.com{0}'.format(str(player_link)))
                new_soup = BeautifulSoup(player_page.content, 'html.parser')
                comments = new_soup.find_all(string=lambda text:isinstance(text,Comment))
                comment = BeautifulSoup(str(comments), 'html.parser')
                first_team = comment.find('table', {'id': 'snap_counts'}).find('tr', {'id': 'snap_counts.{0}'.format(str(year))}).find('td', {'data-stat': 'team'}).get_text()
                possible_list = comment.find('table', {'id': 'snap_counts'}).find_all('tr', {'id': 'snap_counts.'})
                for row in possible_list:
                    if row.find('th').get('csk') == '{0}.2'.format(str(year)):
                        second_team_data.append(first_name)
                        second_team_data.append(last_name)
                        second_team_data.append(year)
                        second_team_data.append(row.find('td', {'data-stat': 'team'}).get_text())
                        second_team_data.append(height)
                        second_team_data.append(weight)
                    else:
                        pass
                first_team_data.append(first_name)
                first_team_data.append(last_name)
                first_team_data.append(year)
                first_team_data.append(first_team)
                first_team_data.append(height)
                first_team_data.append(weight)

            elif item.find('td', {'data-stat':'team'}).get_text() == '3TM':
                year = item.find('td', {'data-stat':'year_id'}).get_text()
                player_link = item.find('td', {'data-stat':'player'}).find('a').get('href')
                player_page = requests.get('https://www.pro-football-reference.com{0}'.format(str(player_link)))
                new_soup = BeautifulSoup(player_page.content, 'html.parser')
                comments = new_soup.find_all(string=lambda text:isinstance(text,Comment))
                comment = BeautifulSoup(str(comments), 'html.parser')
                first_team = comment.find('table', {'id': 'snap_counts'}).find('tr', {'id': 'snap_counts.{0}'.format(str(year))}).find('td', {'data-stat': 'team'}).get_text()
                possible_list = comment.find('table', {'id': 'snap_counts'}).find('tbody').find_all('tr', {'id':'snap_counts.'})
                for row in possible_list:
                    if row.find('th').get('csk') == '{0}.2'.format(str(year)):
                        if not second_team_data:
                            second_team_data.append(first_name)
                            second_team_data.append(last_name)
                            second_team_data.append(year)
                            second_team_data.append(row.find('td', {'data-stat': 'team'}).get_text())
                            second_team_data.append(height)
                            second_team_data.append(weight)
                        else:
                            third_team_data.append(first_name)
                            third_team_data.append(last_name)
                            third_team_data.append(year)
                            third_team_data.append(row.find('td', {'data-stat': 'team'}).get_text())
                            third_team_data.append(height)
                            third_team_data.append(weight)
                    else:
                        pass
                first_team_data.append(first_name)
                first_team_data.append(last_name)
                first_team_data.append(year)
                first_team_data.append(first_team)
                first_team_data.append(height)
                first_team_data.append(weight)
            else:
                year = item.find('td', {'data-stat':'year_id'}).get_text()
                team = item.find('td', {'data-stat':'team'}).get_text()
                first_team_data.append(first_name)
                first_team_data.append(last_name)
                first_team_data.append(year)
                first_team_data.append(team)
                first_team_data.append(height)
                first_team_data.append(weight)

        first_series = pd.Series(first_team_data, index=new_df.columns)
        new_df = new_df.append(first_series, ignore_index = True)
        if not second_team_data:
            pass
        else:
            second_series = pd.Series(second_team_data, index = new_df.columns)
            new_df = new_df.append(second_series, ignore_index = True)
        if not third_team_data:
            pass
        else:
            third_series = pd.Series(third_team_data, index = new_df.columns)
            new_df = new_df.append(third_series, ignore_index = True)

wrong_team_names = ['GNB', 'TAM', 'NWE', 'LAR', 'SFO', 'NOR', 'KAN']
right_team_names = ['GB', 'TB', 'NE', 'LA', 'SF', 'NO', 'KC']

new_df.replace(to_replace = wrong_team_names, value = right_team_names, inplace = True)

print(new_df.head())

new_df.to_csv('player_weights.csv', index_label = 'Index')

role = get_execution_role()

region = boto3.Session().region_name

bucket = 'nyg-hackathon-811331780957'
key = 'ohsaquonuc/player_weights/player_weights'

url = 's3://{}/{}'.format(bucket, key)
boto3.Session().resource('s3').Bucket(bucket).Object(key).upload_file('player_weights.csv')