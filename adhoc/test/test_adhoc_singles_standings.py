import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter
import datetime
from time import sleep

sa = gspread.service_account()
sh = sa.open("TEST SCALPEL Resources (SINGLES)")

schedule_ws = sh.worksheet("SCHEDULE")

schedule = get_as_dataframe(schedule_ws,nrows=90)[['Match','Week', \
    'Player A','Player B','1A','1B','2A','2B','3A','3B']]

played = schedule[pd.notna(schedule['1A'])]

dr={'M':{'Conner McNicholas':[0,0],'Didier Grelin':[0,0],'Evan Frangipane':[0,0],'Kim McGinty':[0,0],'Matt Smith':[0,0],'Nate Hines':[0,0],'Nolan Smyth':[0,0],'Shawnte Hagen':[0,0]},
    'G':{'Conner McNicholas':[0,0],'Didier Grelin':[0,0],'Evan Frangipane':[0,0],'Kim McGinty':[0,0],'Matt Smith':[0,0],'Nate Hines':[0,0],'Nolan Smyth':[0,0],'Shawnte Hagen':[0,0]},
    'P':{'Conner McNicholas':[0,0],'Didier Grelin':[0,0],'Evan Frangipane':[0,0],'Kim McGinty':[0,0],'Matt Smith':[0,0],'Nate Hines':[0,0],'Nolan Smyth':[0,0],'Shawnte Hagen':[0,0]}}

def prntse(se):
    if all([x in se for x in ['Shawnte','Evan']]):
        print(se)

shawntevsevan = False

def printon(sestring):
    if shawntevsevan:
        print(sestring)

for m in range(len(played)):
    match = played.iloc[m]
    A,B = match[['Player A','Player B']]
    if all([x in f'{A} {B}' for x in ['Shawnte','Evan']]):
        shawntevsevan = True
    else:
        shawntevsevan = False

    matchnum = match['Match']
    printon(f'\nmatch {matchnum}:')

    G1A,G1B,G2A,G2B,G3A,G3B = match[['1A','1B','2A','2B','3A','3B']]
    gdict = {1:[G1A,G1B],2:[G2A,G2B],3:[G3A,G3B]}

    for gk,gv in gdict.items():
        tries = 0
        while pd.isna(gv[0]) != pd.isna(gv[1]):
            print(f"Game {i+1} is partially entered, waiting 5 seconds...")
            sleep(5)
            played = schedule[pd.notna(schedule['1A'])]
            match = played.iloc[m]
            gdict[gk] = match[[f'{gk}A',f'{gk}B']]
            tries += 1
            if tries > 2:
                if g == 0:
                    G1A = -999
                    G2A= 999
                elif g == 1:
                    G2A = -999
                    G2B = 999
                else:
                    G3A = -999
                    G3B = 999
                print(f"Game {gk} still half entered, forcing -999,999 values!")
                break
    
    printon(f'1A:{G1A},1B:{G1B}\n2A:{G2A},2B:{G2B}\n3A:{G3A},3B:{G3B}')
            
    if pd.isna(G3A):
        printon('settled in 2 games')
        if G2A > G2B:
            printon(f'{A} beat {B} 2 games to 0')
            dr['M'][A][0]+=1
            dr['M'][B][1]+=1
            dr['G'][A][0]+=2
            dr['G'][B][1]+=2
        else:
            printon(f'{B} beat {A} 2 games to 0')
            dr['M'][A][1]+=1
            dr['M'][B][0]+=1
            dr['G'][A][1]+=2
            dr['G'][B][0]+=2
    else:
        printon('settled in 3 games')
        if G3A > G3B:
            printon(f'{A} beat {B} 2 games to 1')
            dr['M'][A][0]+=1
            dr['M'][B][1]+=1
            dr['G'][A][0]+=2
            dr['G'][A][1]+=1
            dr['G'][B][1]+=2
            dr['G'][B][0]+=1
        else:
            printon(f'{B} beat {A} 2 games to 1')
            dr['M'][B][0]+=1
            dr['M'][A][1]+=1
            dr['G'][B][0]+=2
            dr['G'][B][1]+=1
            dr['G'][A][1]+=2
            dr['G'][A][0]+=1
    
    PA = pd.Series([G1A,G2A,G3A]).sum().astype(int)
    PB = pd.Series([G1B,G2B,G3B]).sum().astype(int)
    dr['P'][A]=[dr['P'][A][0]+PA,dr['P'][A][1]+PB]
    dr['P'][B]=[dr['P'][B][0]+PB,dr['P'][B][1]+PA]
    printon(f'{A} scored {PA} points\n{B} scored {PB} points')

df_standings = pd.DataFrame(Counter(pd.concat([played['Player A'],played['Player B']])).items(),columns=['Player','MP']).sort_values('Player')

df_standings['MW']=[dr['M'][x][0] for x in df_standings.Player]
df_standings['ML']=[dr['M'][x][1] for x in df_standings.Player]
df_standings['MR']=(df_standings.MW/df_standings.MP).round(4)

df_standings['GW']=[dr['G'][x][0] for x in df_standings.Player]
df_standings['GL']=[dr['G'][x][1] for x in df_standings.Player]
df_standings['GP']=df_standings[['GW','GL']].sum(axis = 1, skipna = True)
df_standings['GR']=(df_standings.GW/df_standings.GP).round(4)

df_standings['PF']=[dr['P'][x][0] for x in df_standings.Player]
df_standings['PA']=[dr['P'][x][1] for x in df_standings.Player]
df_standings['PD']=(df_standings.PF-df_standings.PA)
df_standings['PR']=(df_standings.PF/(df_standings.PF+df_standings.PA)).round(4)

df_standings = df_standings.sort_values(['MR','MW','GR','GW','PR','PF'],ascending=[False,False,False,False,False,False])
df_standings['Rank']=range(1,1+len(df_standings))
df_standings = df_standings[['Rank','Player','MP','MW','ML','MR','GP','GW','GL','GR','PF','PA','PD','PR']]
print(df_standings.reset_index(drop=True).to_string())

current_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %p")
f = open("/Users/conner/pickleball_league/singles_updatelog.txt", "a")
f.write(f'{len(played)} matches in standings | UPDATING ADHOC | {current_ts}\n')
standings_ws = sh.worksheet("STANDINGS")
set_with_dataframe(standings_ws, df_standings, row=2, col=2)
