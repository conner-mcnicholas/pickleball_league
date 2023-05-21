import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter
import datetime

sa = gspread.service_account()
sh = sa.open("SCALPEL Resources (DOUBLES)")

schedule_ws = sh.worksheet("SCHEDULE")
schedule = get_as_dataframe(schedule_ws,nrows=109)[['Match','Week','Leg', \
    'T-A','T-B','Player A-1','Player A-2','Player B-1','Player B-2', \
    '1A','1B','2A','2B','3A','3B']]
played = schedule[pd.notna(schedule['1A'])].loc[schedule.Scheduled != 'Simulated']

current_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %p")
f = open("/Users/conner/pickleball_league/stats_updatelog.txt", "r+")
s=f.readlines()[-1]
prev_mp = int(s[:s.find(' ')])
print(f'stats reflects {len(played)} matches played, compared to previous run of {prev_mp}')
if prev_mp == len(played):
    f.write(f"{len(played)} matches already accounted for | QUITTING | {current_ts}\n")
    quit()

players_ws = sh.worksheet("PLAYER DATASHEET")
df_players = get_as_dataframe(players_ws,nrows=pd.notna(get_as_dataframe(players_ws).NAME).sum()) \
    [['NAME','TEAM','r-DUPR','p-DUPR','EXPERIENCE','AGE','GENDER','CAP']]
df_players.loc[pd.isna(df_players['p-DUPR']),'p-DUPR'] = df_players['p-DUPR'].mean()
players = list(df_players.NAME)
dr={'M':{},'G':{},'P':{}}
for p in players:
    for k in ['M','G','P']:
        dr[k][p]=[0,0]

allplayers = []
for p in ['Player A-1','Player A-2','Player B-1','Player B-2']:
    for pi in list(played[p]):
        allplayers.append(pi)
df_stats = pd.DataFrame(Counter(allplayers).items(),columns=['Player','MP']).sort_values('Player').reset_index(drop=True)
df_stats = pd.concat([df_stats,df_players['p-DUPR']],axis=1)[['Player','p-DUPR','MP']]

for m in range(len(played)):
    match = played.iloc[m]

    matchnum = match['Match']
    A1,A2,B1,B2 = match[['Player A-1','Player A-2','Player B-1','Player B-2',]]
    G1A,G1B,G2A,G2B,G3A,G3B = match[['1A','1B','2A','2B','3A','3B']]
    
    #print(f'\nmatch #{matchnum}:')
    #print(f'1A:{G1A},1B:{G1B}\n2A:{G2A},2B:{G2B}\n3A:{G3A},3B:{G3B}')
    PA = pd.Series([G1A,G2A,G3A]).sum().astype(int)
    PB = pd.Series([G1B,G2B,G3B]).sum().astype(int)
    if pd.isna(G3A):
        #print('settled in 2 games')
        if G2A > G2B:
            #print(f'{A} beat {B} 2 games to 0')
            dr['M'][A1][0]+=1
            dr['M'][B1][1]+=1
            dr['G'][A1][0]+=2
            dr['G'][B1][1]+=2
            
            dr['M'][A2][0]+=1
            dr['M'][B2][1]+=1
            dr['G'][A2][0]+=2
            dr['G'][B2][1]+=2
        else:
            #print(f'{B} beat {A} 2 games to 0')
            dr['M'][A1][1]+=1
            dr['M'][B1][0]+=1
            dr['G'][A1][1]+=2
            dr['G'][B1][0]+=2

            dr['M'][A2][1]+=1
            dr['M'][B2][0]+=1
            dr['G'][A2][1]+=2
            dr['G'][B2][0]+=2
    else:
        #print('settled in 3 games')
        if G3A > G3B:
            #print(f'{A} beat {B} 2 games to 1')
            dr['M'][A1][0]+=1
            dr['M'][B1][1]+=1
            dr['G'][A1][0]+=2
            dr['G'][A1][1]+=1
            dr['G'][B1][1]+=2
            dr['G'][B1][0]+=1

            dr['M'][A2][0]+=1
            dr['M'][B2][1]+=1
            dr['G'][A2][0]+=2
            dr['G'][A2][1]+=1
            dr['G'][B2][1]+=2
            dr['G'][B2][0]+=1
        else:
            #print(f'{B} beat {A} 2 games to 1')
            dr['M'][B1][0]+=1
            dr['M'][A1][1]+=1
            dr['G'][B1][0]+=2
            dr['G'][B1][1]+=1
            dr['G'][A1][1]+=2
            dr['G'][A1][0]+=1

            dr['M'][B2][0]+=1
            dr['M'][A2][1]+=1
            dr['G'][B2][0]+=2
            dr['G'][B2][1]+=1
            dr['G'][A2][1]+=2
            dr['G'][A2][0]+=1
    #print(f'Team {A} scored {PA} points\nTeam {B} scored {PB} points')
    dr['P'][A1]=[dr['P'][A1][0]+PA,dr['P'][A1][1]+PB]
    dr['P'][B1]=[dr['P'][B1][0]+PB,dr['P'][B1][1]+PA]

    dr['P'][A2]=[dr['P'][A2][0]+PA,dr['P'][A2][1]+PB]
    dr['P'][B2]=[dr['P'][B2][0]+PB,dr['P'][B2][1]+PA]

df_stats['MW']=[dr['M'][x][0] for x in df_stats.Player]
df_stats['ML']=[dr['M'][x][1] for x in df_stats.Player]
df_stats['MR']=(df_stats.MW/df_stats.MP).round(4)

df_stats['GW']=[dr['G'][x][0] for x in df_stats.Player]
df_stats['GL']=[dr['G'][x][1] for x in df_stats.Player]
df_stats['GP']=df_stats[['GW','GL']].sum(axis = 1, skipna = True)
df_stats['GR']=(df_stats.GW/df_stats.GP).round(4)

df_stats['PF']=[dr['P'][x][0] for x in df_stats.Player]
df_stats['PA']=[dr['P'][x][1] for x in df_stats.Player]
df_stats['PD']=(df_stats.PF-df_stats.PA)
df_stats['PF/G']=(df_stats.PF/(df_stats.GP)).round(4)
df_stats['PA/G']=(df_stats.PA/(df_stats.GP)).round(4)
df_stats['PD/G']=(df_stats.PD/(df_stats.GP)).round(4)
df_stats['PR']=(df_stats.PF/(df_stats.PF+df_stats.PA)).round(4)

df_stats = df_stats.sort_values(['MR','MW','GR','GW','PR','PF'],ascending=[False,False,False,False,False,False])
df_stats['Rank']=range(1,1+len(players))
df_stats = df_stats[['Rank','Player','MP','MW','ML','MR','GP','GW','GL','GR','PF','PA','PD','PF/G','PA/G','PD/G','PR']]
print(df_stats.reset_index(drop=True).to_string())

"""
add:
teams
actual DUPR
league DUPR (calculate like spreadsheet)
% 3rd match losses
% 3rd match wins
point rate wins 
point rate losses
per game -> Skill Mismatch Ratio = (partner DUPR/(average opponent DUPR)
difficulty = (average skill mismatch ratio), normalized from 0-1 where
 1 = hardest match is worst player as partner against 2 best players
 0 = easiest match is best player as partner against 2 worst players
"""

f.write(f'{len(played)} matches now in stats | EDITING SHEET | {current_ts}\n')
stats_ws = sh.worksheet("PLAYER STATS")
set_with_dataframe(stats_ws, df_stats, row=2, col=2)
