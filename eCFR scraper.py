import urllib.request as REQ
import json
from bs4 import BeautifulSoup as BS
import mysql.connector

#connects to database (configure with details)
mydb = mysql.connector.connect(
  host="",
  user="",
  password= '',
  database = '')
db = mydb.cursor()

#fetches date and loads structure json & content XML
dateURL = 'https://www.ecfr.gov/api/versioner/v1/titles.json'
dateData = json.load(REQ.urlopen(dateURL))
dateData['meta']['date']
date = dateData['meta']['date']

ecfrStructureURL = f'https://www.ecfr.gov/api/versioner/v1/structure/{date}/title-17.json'
ecfrStructureData = json.load(REQ.urlopen(ecfrStructureURL))
ecfrChapterII= ecfrStructureData['children'][1]

#fetching and then parsing XML for each section is extremely time consuming (about 10x more slower), 
#so better to fetch and parse the entire XML at once and then reference specific objects when needed
ecfrContentURL = f'https://www.ecfr.gov/api/versioner/v1/full/{date}/title-17.xml'
ecfrContentXML = BS(REQ.urlopen(ecfrContentURL), 'lxml-xml')

#empty rows for table input
chapterTableRows=[]
subchapterTableRows=[]
partTableRows=[]
subpartTableRows = []
subjectgroupTableRows = []
sectionTableRows = []

#initialises empty data names
chapterName = subchapterName = partName = subpartName = subjectgroupName = sectionName = ''

#function takes a json tree and an iteration count (start from 0 when initially calling)
def parse(file, start):

    global chapterName
    global subchapterName
    global partName
    global subpartName
    global subjectgroupName
    global sectionName
    global sectionTitle
    global sectionContent

    #correctly identifies level in the hierarchy so that the function can be called for just one chapter if desired
    if file['type']== 'chapter':
        chapterName = file['identifier']
        chapterTitle = file['label']
        chapterTableRows.append([chapterName,chapterTitle])

    #recursively traverses the entire tree
    
    #iterates on every child using a for loop
    for i in range(0,len(file['children'])):

        #discounts removed nodes
        if file['children'][i]['reserved'] == False:

            #identifying what level of the hierarchy is currently being parsed and storing data accordingly

            if file['children'][i]['type'] == 'chapter':
                chapter = file['children'][i]
                chapterName = chapter['identifier']
                chapterTitle = chapter['label']

                #at each level any information stored about previous lower level data is cleared
                subchapterName = partName=subpartName=subjectgroupName = sectionName=sectionTitle=sectionContent=''

                #inputs data into a row and inserts that row into the 'table' of rows
                chapterRow = [chapterName,chapterTitle]
                chapterTableRows.append(chapterRow) 

                #calls the function on each child as i increments
                parse(chapter, i)

            if file['children'][i]['type'] == 'subchapter':

                #extracts data
                subchapter = file['children'][i]
                subchapterName = subchapter['identifier']
                subchapterTitle = subchapter['label']

                #resets existing lower level data
                partName=subpartName=subjectgroupName = sectionName=sectionTitle=sectionContent=''

                #inputs data into a row and inserts that row into the 'table' of rows
                subchapterRow = [chapterName,subchapterName,subchapterTitle]
                subchapterTableRows.append(subchapterRow) 

                #calls the function on each child as i increments
                parse(subchapter, i)

            if file['children'][i]['type'] == 'part':

                #extracts data
                part = file['children'][i]
                partName = part['identifier']
                partTitle = part['label']

                #resets existing lower level data
                subpartName=subjectgroupName = sectionName=sectionTitle=sectionContent=''

                #inputs data into a row and inserts that row into the 'table' of rows
                partRow = [chapterName,subchapterName,partName,partTitle]
                partTableRows.append(partRow) 

                #calls the function on each child as i increments
                parse(part, i)

            if file['children'][i]['type'] == 'subpart':

                #extracts data
                subpart = file['children'][i]
                subpartName = subpart['identifier']
                subpartTitle = subpart['label']

                #resets existing lower level data
                subjectgroupName = sectionName=sectionTitle=sectionContent=''

                #inputs data into a row and inserts that row into the 'table' of rows
                subpartRow = [chapterName,subchapterName,partName,subpartName,subpartTitle]
                subpartTableRows.append(subpartRow) 

                #calls the function on each child as i increments
                parse(subpart, i)

            if file['children'][i]['type'] == 'subject_group':

                #extracts data
                subjectgroup = file['children'][i]
                subjectgroupName = subjectgroup['label']
                subjectgroupID = subjectgroup['identifier']

                #resets existing lower level data
                sectionName=sectionTitle=sectionContent=''

                #inputs data into a row and inserts that row into the 'table' of rows
                subjectgroupRow = [chapterName,subchapterName,partName,subpartName,subjectgroupID,subjectgroupName]
                subjectgroupTableRows.append(subjectgroupRow) 

                #calls the function on each child as i increments
                parse(subjectgroup, i)

            if file['children'][i]['type'] == 'section':

                #BASE CASE for recursive structure as sections are leaf nodes

                #extracts data
                section = file['children'][i]
                sectionName = section['identifier']
                sectionContent = ''
                sectionTitle = section['label']

                #finds the section in XML
                content = ecfrContentXML.find(N=sectionName)

                #for each paragraph, unwraps style tags and concantenates into a single piece of text
                paras = content.find_all('P')
                for para in paras:
                    for ital in para.find_all('I'):
                        para.I.unwrap()
                    for bold in para.find_all('B'):
                        para.B.unwrap()
                    for etag in para.find_all('E'):
                        para.E.unwrap()
                    sectionContent += para.text
                    
                #inputs data into a row and inserts that row into the 'table' of rows
                sectionRow = [chapterName,subchapterName,partName,subpartName,subjectgroupName,sectionName,sectionTitle,sectionContent]
                sectionTableRows.append(sectionRow) 


               
                    
#parses just chapter two, code can also parse the entire part                   
parse(ecfrChapterII, 0)

#function to push data into SQL database
def SQL():

    #clears the existing database
    tables = ['chapters','subchapters','parts','subparts','subject_groups','sections']
    for table in tables:
        tableClear = f"DELETE FROM {table}"
        db.execute(tableClear)

    #statements to insert data into respective tables   
    chapterInsert = "INSERT INTO chapters (ID, Title) VALUES (%s, %s)"        
    subchapterInsert = "INSERT INTO subchapters (Chapter, ID, Title) VALUES (%s, %s, %s)"        
    partInsert = "INSERT INTO parts (Chapter,Subchapter, ID, Title) VALUES (%s, %s, %s, %s)"
    subpartInsert = "INSERT INTO subparts (Chapter,Subchapter,Part, ID, Title) VALUES (%s, %s, %s, %s, %s)"
    subjectgroupInsert = "INSERT INTO subject_groups (Chapter,Subchapter,Part,Subpart, ID, Title) VALUES (%s, %s, %s, %s, %s, %s)"
    sectionInsert = "INSERT INTO sections (Chapter,Subchapter,Part,Subpart,Subject_Group, ID, Title, Content) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    #inserts data
    db.executemany(chapterInsert, chapterTableRows)
    db.executemany(subchapterInsert, subchapterTableRows)
    db.executemany(partInsert, partTableRows)
    db.executemany(subpartInsert, subpartTableRows)
    db.executemany(subjectgroupInsert, subjectgroupTableRows)
    db.executemany(sectionInsert, sectionTableRows)
    mydb.commit()
    
#pushes data
SQL()

