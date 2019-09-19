# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 10:49:16 2019

@author: mwilcox
"""

import time
import pandas as pd
import requests
import collections
from bs4 import BeautifulSoup, Tag, NavigableString


def tagstringextract(x):
    """extract string from NavigableString"""
    if isinstance(x, NavigableString):
        return x.lstrip().rstrip()
    elif isinstance(x, Tag):
        return x.get_text().lstrip().rstrip()
    elif isinstance(x, list):
        return ''.join([tagstringextract(a) for a in x])
    else:
        raise Exception('Detected neither Tag nor NavigableString')


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def maldfclean(raw):
    """Take in list of dictionaries from scraping, convert to clean dataframe"""
    maldf = pd.DataFrame(raw)
    
    # set key, remove colon from column names
    maldf = maldf.set_index('key')
    maldf.columns = maldf.columns.str.replace(':', '')
    
    # convert episode count
    maldf['Episodes'] = maldf['Episodes'].replace({',',''})
    maldf['Episodes'] = maldf['Episodes'].replace('Unknown', '0')
    maldf.Episodes = maldf.Episodes.astype('int32')
    
    # convert Members
    maldf['Members'] = maldf['Members'].str.replace(',','').astype('int64')
    
    # convert Favorites
    maldf['Favorites'] = maldf['Favorites'].str.replace(',','').astype('int64')
    
    # extract Score 
    maldf['ScoreExtract'] = maldf.Score.str.extract('(\d+.\d+)').astype('float64')
    
    # extract ScoreRaters
    maldf['ScoreRaters'] = maldf.Score.str.extract('(?<=by)(.*)(?=users)')
    maldf['ScoreRaters'] = maldf['ScoreRaters'].str.replace(',', '').astype('int64')
    
    # maldf['FirstAired'] = maldf['Aired'].str.findall('(\w{3} \d{1,2}, \d{4})')
    return maldf


def sequeldfclean(raw):
    """Convert SequelKey relations to pandas DataFrame"""
    
    
    for i in raw:
        i[1] = i[1].replace(':', '')
        

    df = pd.DataFrame(raw, columns = ['PrimaryKey', 'RelationType', 'RelationKey'])
    df = df[df.RelationType != 'Adaptation']
    
    return df

def malkey(x, aslist = True):
    """returns an array of primary key[s] from url string[s]"""

    if isinstance(x, str):
        if aslist:
            return [int(s) for s in x.split('/') if s.isdigit()]
        else:
            return [int(s) for s in x.split('/') if s.isdigit()][0]
    
    elif isinstance(x, list):
        if len(x) > 1:
            return [malkey(y, aslist = False) for y in x]  
        else:
            return malkey(x)
    
    elif isinstance(x, Tag):
        if len(x) == 1:
            if isinstance(x.contents[0], Tag):
                # returning ['href'] gives a string, so use aslist argument
                if aslist:
                    return malkey(x.contents[0]['href'], aslist = True)
                else:
                    return malkey(x.contents[0]['href'], aslist = False)
            elif isinstance(x.contents[0], NavigableString):
                return malkey(x['href'], aslist = False)
            else:
                raise Exception('Detected neither Tag nor NavigableString')
        else:
            return [malkey(s, aslist = False) for s in x if isinstance(s, Tag) and len(s) > 0]
                
    else:
        raise Exception('Detected neither str nor list of str')
        


def malscrape(urllist, minlooptime = 2):
    """Main function for scraping MAL anime sites from urllist"""
    
    failedlist = []
    sequelkeys = []
    mallist = []
    failedcount = 0
    maxindex = len(urllist)

    for idx, url in enumerate(urllist):
        # start timer
        timestart = time.time()
        
        try:
            page = requests.get(url,timeout=5)
            page.raise_for_status()
        except requests.exceptions.RequestException as e:
            failedcount += 1 
            print(e)
            print('Current Failed Count: ' + str(failedcount))
            failedlist.append(url)
            
            if time.time() - timestart < minlooptime:
                time.sleep(minlooptime - (time.time() - timestart))
            continue
    
        
        # get primary key, start new row
        animekey = malkey(url, aslist = False)
        newrow = {'key': animekey}
        
        # create soup
        anime = BeautifulSoup(page.content, 'html.parser')
        
        # get title
        title = {'Title': anime.select('.h1 span')[0].string}
        newrow.update(title)
        
        # get sidebar information
        sidebar = [a for a in 
                   [[x for x in entry if x != '\n'] for entry in anime.select('.js-scrollfix-bottom div')]
                   if len(a) > 1 and a[0].name == 'span']
        sidebardict = dict([entry[0].get_text(), tagstringextract(entry[1:])] for entry in sidebar)
        newrow.update(sidebardict)
        
        # add to global list
        mallist.append(newrow)
        
        # related
        relatedanime = anime.select('.anime_detail_related_anime .borderClass')
        relatedlist = [[animekey, alpha[0].text, y] for alpha in chunks(relatedanime, 2) for y in malkey(alpha[1])]
        
        sequelkeys += relatedlist
        
        # pause if too fast
        if time.time() - timestart < minlooptime:
            time.sleep(minlooptime - (time.time() - timestart))
        print('Processed anime [key = ' + str(animekey) + '], ' 
              + '%d' % (idx+1) + '/' + str(maxindex) + ', ' + '%.2f' % (100*(idx+1)/maxindex) + '%: in ' 
              + '%.4f' % (time.time() - timestart) + ' seconds.')
        
    print('Scraping Complete')
    
    mallist = maldfclean(mallist)
    sequelkeys = sequeldfclean(sequelkeys)
    
    ScrapeResult = collections.namedtuple('ScrapeResult', ['mallist', 'sequelkeys'])
    y = ScrapeResult(mallist, sequelkeys)
    return y

def updatesitemap(oldlist, minlooptime = 2):
    """Update saved sitemap with newly-added anime urls"""
    try:
        global updatelist
    except NameError:
        updatelist = []
        
    # updatelist = []

    lastmaxurl = oldlist[-1]
    lastmaxindex = malkey(lastmaxurl, aslist=False)
    
    page = requests.get('https://myanimelist.net/anime.php?o=9&c%5B0%5D=a&c%5B1%5D=d&cv=2&w=1')
    firstpage = BeautifulSoup(page.content, 'html.parser')
    table = firstpage.select('.borderClass')
    urllist = [x.contents[1].contents[1]['href'] for x in table if len(x) == 3]
    
    if malkey(urllist[49], aslist=False) > lastmaxindex:
        increment = 0
    
        while malkey(urllist[49], aslist=False) > lastmaxindex:
            timestart = time.time()
            updatelist.extend(urllist)
            
            increment += 50
            page = requests.get('https://myanimelist.net/anime.php?o=9&c%5B0%5D=a&c%5B1%5D=d&cv=2&w=1&show=' + str(increment))
            print(page)
            nextpage = BeautifulSoup(page.content, 'html.parser')
        
            table = nextpage.select('.borderClass')
            urllist = [x.contents[1].contents[1]['href'] for x in table if len(x) == 3]
            print(malkey(urllist[49], aslist=False))
    
            if time.time() - timestart < minlooptime:
                time.sleep(minlooptime - (time.time() - timestart))
    
    print('Reached converged page')
    
    lastpage = urllist[0:urllist.index(lastmaxurl)]
    updatelist.extend(lastpage)
    updatelist.reverse()
    
    updatedlist = oldlist.extend(updatelist)
    
    return updatedlist




