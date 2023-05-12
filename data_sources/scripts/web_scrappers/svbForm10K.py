from bs4 import BeautifulSoup
# import requests
# from requests_html import HTMLSession
from pprint import pprint
import os
import json
import re

# URL='https://www.sec.gov/Archives/edgar/data/719739/000071973923000021/sivb-20221231.htm'
# pageHTML=requests.get(URL)

# soup=BeautifulSoup(pageHTML.content, 'html.parser')

path=os.path.relpath('/Users/axyom7/Desktop/ds4a-capstone/data_sources/data/svb_Form10K_body.html')

with open(path, 'r') as readerFile:
    soup=BeautifulSoup(readerFile, 'html.parser')
    tablesList=soup.find_all('table')
    

    

            # for cell in row.find('td'):
            #     print(cell.text)
        #     pprint(row.text)

            # tabledata=[[c.text for c in row.find('td')] for row in table.find('tr')]
    # for row in tables.find('tr'):
    #     pprint(row)

    # tableElements=soup.find_all('table')
    # tdElements=soup.find_all('td')

writeToPath=os.path.relpath('/Users/axyom7/Desktop/ds4a-capstone/data_sources/data/tableData.json')
with open(writeToPath, 'w') as divFile:
    tableDataList=[]
    spanTagData=[]
    for table in tablesList:
        tableData=table.text
        tableDataList.append(tableData)
        spanTagList=table.find_all('span')
        for spanTag in spanTagList:
            if spanTag.text=='':
                pass
            else:
                rawData=spanTag.text
                rawDataEN=rawData.encode('ascii', 'ignore')
                rawDataDE=rawDataEN.decode()
                cleanData=rawDataDE.replace('\n','')
                spanTagData.append(cleanData)
    removeSpacesInList=[tag.strip() for tag in spanTagData]
    cleanedDataList=[tag for tag in removeSpacesInList if tag !='']
    print(len(cleanedDataList))
    spanHTMLPath=os.path.relpath('/Users/axyom7/Desktop/ds4a-capstone/data_sources/data/spanTags.html')
    with open(spanHTMLPath, 'w') as spanTagHTMLFile:
        spanTagHTMLFile.write(str(spanTagList))
    # convert list to json
    jsonDataList=json.dumps(cleanedDataList)
    # write to json file
    divFile.write(jsonDataList)
#     tableHTMLData=[table.text for table in tablesList]
#     # tableData=[td.text for td in tableHTMLData]
#     print(len(tableHTMLData))
    # for table in tablesList:
    #     pprint(table.text)
    # divFile.write(str(tableHTMLData))
    # for table in tablesList:
    #     for row in table.find('tr'):
    #         print(row.text)
#     i=0
#     tagDict={}
#     # divFile.write({:tag.get_text() for tag in soup.find_all('table'), i+=1})
#     for tableTag in soup.find_all('table'):
        
#         trDict={}
#         trTagCounter=0
#         tdDict={}
#         tdTagCounter=0
#         # spanDict={}
#         # spanTagCounter=0
        
#         for trTag in tableTag.find_all('td'):
#             rawTRTag=trTag.get_text()
#             trTagEN=rawTRTag.encode('ascii', 'ignore')
#             trTagDE=trTagEN.decode()
#             cleanedTRTagDE=trTagDE.strip()
            
#             # if cleanedTRTagDE=='':
#             #     pass
#             # else:
#             #     trDict[f'table_row_{trTagCounter}']={'data_point':cleanedTRTagDE}
#             #     trTagCounter+=1
#             # elif cleanedTRTagDE.isdigit():
#             #     # trDict[f'table_row_{trTagCounter}'][f'data_point_{trTagCounter}']=cleanedTRTagDE.strip()
#             #     pass
#             # # elif cleanedTRTagDE.isdigit():
#             # #     pass
#             # #     # rawTRTag=trTRTagString
#             # #     trDict[f'table_row_{trTagCounter}']='abcdefg'#''.join(rawTRTag.split())
#             # #     trTagCounter+=1
#             # else:
#             #     try:
#             #         float(cleanedTRTagDE)
#             #         int(cleanedTRTagDE)
#             #         trDict[f'table_row_{trTagCounter}']=[cleanedTRTagDE]#''.join(trTagDE.split())
#             #         trTagCounter+=1
#             #     except ValueError:
#             #         pass
            
#                 # rawTRTag=trTag.get_text()
#                 # trDict[f'table_row_{trTagCounter}']=[cleanedTRTagDE, cleanedTRTagDE.isdigit()]#''.join(trTagDE.split())
                

                
#         # for tdTag in tableTag.find_all('td'):
#         #     if str(tdTag.get_text()) == "":
#         #         pass
#         #     else:
#         #         rawTDTag=tdTag.get_text()
#         #         tdDict[f'data_point_{tdTagCounter}']=''.join(rawTDTag.split())
#         #         tdTagCounter+=1
#         # for spanTag in tableTag.find_all('span'):
#         #     if str(spanTag.get_text()) == "":
#         #         pass
#         #     else:
#         #         spanDict[f'span_tag_{spanTagCounter}']=str(spanTag.get_text())
#         #         spanTagCounter+=1
#         tagDict[f'table_{i}']=[trDict]#, tdDict]
        
            
            
        

#         i+=1
    # tagDictJSONIFIED=json.dumps(tablesList, indent=4)
    # pprint(tagDictJSONIFIED)
    # divFile.write(tagDictJSONIFIED)
    # for tag in soup.find_all('table'):
    #     if tag.get_text() =='':
    #         pass
    #     else:
    #         # textArray.append(tag.get_text())
    #         divFile.write(str({i: str(tag.get_text())}))
    #         divFile.write('\n')
    #         i+=1
