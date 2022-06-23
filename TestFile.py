__Project__ = 'Crawl Input Creation Automation'
#=========Import Function========#
import pandas as pd,glob,random,json

#==============To Get CSV files list from a folder===============#
path = r'C:\Users\santhosh.s3\Downloads\Automation'
csv_files = glob.glob(path + "/*.csv")
df_list = (pd.read_csv(file,sep=',') for file in csv_files)
#==============Concatenate all DataFrames================#
big_df   = pd.concat(df_list, ignore_index=False,sort=True)
Keys = pd.DataFrame(big_df)
#==============Capturing data from inputfile===============#
zipcode = Keys['Zipcode']
item_number = Keys["Sam's Item#"]
region = Keys["Region"]
sam_name = Keys["Sam's Name"]
#=============Looping all the columns=============#
Sams = []
for i,j,k,l in zip(zipcode,item_number,region,sam_name):
    data = {}
    finalZipcode = ''
    #==========calculating zipcode length to identify 3, 4 digit zipcode=======#
    zipLen = str(i)
    ZipcodeLength =  len(str(zipLen))
    if str(ZipcodeLength) == '4':
        finalZipcode = '0' + str(zipLen)
    elif str(ZipcodeLength) == '3':
        finalZipcode = '00' + str(zipLen)
    elif str(ZipcodeLength) == '5':
        finalZipcode = str(zipLen)
    data["sams_name"] = str(l).replace("'","")
    data["zipcode"] = ZipcodeData =  finalZipcode.split('.')[0]
    data["region"] = str(k)
    data["sams_item"] = str(j)
   #=========To concatenate zipcode_itemnumber_region===========#
    data["cat"] = '{}_{}_{}'.format(str(j),str(ZipcodeData),str(k))
    Sams.append(data)
x = str(Sams).replace("'",'"')
# if str('nan') not in str(x):
df = pd.read_json(x)
# print(df)
#     #==========Saving client input in local path | The folder should be empty========#
df.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Client Input\Client Input_11.csv", encoding='utf-8', index=False, sep=',') #======Convertting json to csv=======#

# #==============Creating input file===============#
# database_file = pd.read_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Database\Crawl Database.csv", sep=',')
# db_frame = pd.DataFrame(database_file)
# pro_url = db_frame['product_url']
# storeid = db_frame['Store ID']
# zipcodeinp = db_frame['Zipcode']
# state = db_frame['State Name']
# state_id = db_frame['STATE']
# store_name = db_frame["Sam's Name"]
# region_name = db_frame['Region name']
# pro_id = db_frame['Product_id']
# inpList = []
# for sto,zipc,state_name,stateid,sto_name,p_url,reg_name,p_id in zip(storeid,zipcodeinp,zipcodeinp,state_id,store_name,pro_url,region_name,pro_id):
#     inputDict = {}
#     ranNum = random.randint(11111,99999)
#     ranNumpxvid = random.randint(1111,9999)
#     inputDict['url'] = 'https://www.samsclub.com/api/node/vivaldi/browse/v2/products/{}?includeOptical=true&type=LARGE&clubId={}'.format(str(p_id),str(sto)).replace('.0','')   #====need to capture product id fro product url column
#     inputDict['domain'] = 'samsclub'
#     inputDict['webMethod'] = 'get'
#     inputDict['type'] = 'product_page'
#     inputDict['pageDepth'] = 'product_page'
#     inputDict['gatewayType'] = 'PYTHONREQUEST'
#     inputDict['validateParsedOutput'] = 'FALSE'
#     inputDict['fetchNextCrawlUrl'] = 'TRUE'
#     inputDict['uniqueIdentifier'] = '{}_{}'.format(str(ranNum),str(zipc))
#     inputDict['product_url'] = str(p_url)
#     inputDict['variation_flag'] = 'default_flag'
#     inputDict['pxvid'] = str(ranNumpxvid)
#     inputDict['state'] = str(state_name)
#     inputDict['state_id'] = str(stateid)
#     inputDict['store'] = str(sto_name).replace("'",'')
#     inputDict['ebags_unique_identifier'] = str(zipc)
#     inputDict['zipcode_input'] = str(sto)
#     inputDict['region_name'] = str(reg_name)
#     inputDict['main_url'] = 'https://www.samsclub.com/api/node/vivaldi/browse/v2/products?includeOptical=true{}'.format(inputDict['uniqueIdentifier'])
#     inpList.append(inputDict)
# outputInp = str(inpList).replace("'",'"')
# # ==========Saving input in local path based on the regions========#
# finalOutput = json.loads(outputInp)
# inpZList = []
# for val in finalOutput:
#     regName = val['region_name']
#     # ==========All Regions Name NW,Meat,Produce sams,BA,MW,NE,TE,SE,LA,SD=========#
#     if 'NW'  in str(regName):
#         InpData = str(val).replace("'",'"')
#         inpZList.append(InpData)
#         regInpData = str(inpZList).replace("'",'')
#         dbf = pd.read_json(regInpData)
#         dbf.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Final Input\Input_Sams_Club_NW_PDP.csv", encoding='utf-8', index=False, sep=',')
# inpZList1 = []
# for val in finalOutput:
#     regName = val['region_name']
#     if 'Meat' == str(regName):
#         InpData = str(val).replace("'",'"')
#         inpZList1.append(InpData)
#         regInpData = str(inpZList1).replace("'",'')
#         dbf = pd.read_json(regInpData)
#         dbf.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Final Input\Input_Sams_Club_Meat_PDP.csv", encoding='utf-8', index=False, sep=',')
# inpZList2 = []
# for val in finalOutput:
#     regName = val['region_name']
#     if 'Produce sams'  in str(regName):
#         InpData = str(val).replace("'",'"')
#         inpZList2.append(InpData)
#         regInpData = str(inpZList2).replace("'",'')
#         dbf = pd.read_json(regInpData)
#         dbf.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Final Input\Input_Sams_Club_Produce sams_PDP.csv", encoding='utf-8', index=False, sep=',')
# inpZList3 = []
# for val in finalOutput:
#     regName = val['region_name']
#     if 'BA'  in str(regName):
#         InpData = str(val).replace("'",'"')
#         inpZList3.append(InpData)
#         regInpData = str(inpZList3).replace("'",'')
#         dbf = pd.read_json(regInpData)
#         dbf.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Final Input\Input_Sams_Club_BA_PDP.csv", encoding='utf-8', index=False, sep=',')
# inpZList4 = []
# for val in finalOutput:
#     regName = val['region_name']
#     if 'MW'  in str(regName):
#         InpData = str(val).replace("'", '"')
#         inpZList4.append(InpData)
#         regInpData = str(inpZList4).replace("'", '')
#         dbf = pd.read_json(regInpData)
#         dbf.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Final Input\Input_Sams_Club_MW_PDP.csv",encoding='utf-8', index=False, sep=',')
# inpZList5 = []
# for val in finalOutput:
#     regName = val['region_name']
#     if 'NE'  in str(regName):
#         InpData = str(val).replace("'", '"')
#         inpZList5.append(InpData)
#         regInpData = str(inpZList5).replace("'", '')
#         dbf = pd.read_json(regInpData)
#         dbf.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Final Input\Input_Sams_Club_NE_PDP.csv",encoding='utf-8', index=False, sep=',')
# inpZList6 = []
# for val in finalOutput:
#     regName = val['region_name']
#     if 'TE'  in str(regName):
#         InpData = str(val).replace("'", '"')
#         inpZList6.append(InpData)
#         regInpData = str(inpZList6).replace("'", '')
#         dbf = pd.read_json(regInpData)
#         dbf.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Final Input\Input_Sams_Club_TE_PDP.csv",encoding='utf-8', index=False, sep=',')
# inpZList7 = []
# for val in finalOutput:
#     regName = val['region_name']
#     if 'SE'  in str(regName):
#         InpData = str(val).replace("'", '"')
#         inpZList7.append(InpData)
#         regInpData = str(inpZList7).replace("'", '')
#         dbf = pd.read_json(regInpData)
#         dbf.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Final Input\Input_Sams_Club_SE_PDP.csv",encoding='utf-8', index=False, sep=',')
# inpZList8 = []
# for val in finalOutput:
#     regName = val['region_name']
#     if 'LA'  in str(regName):
#         InpData = str(val).replace("'", '"')
#         inpZList8.append(InpData)
#         regInpData = str(inpZList8).replace("'", '')
#         dbf = pd.read_json(regInpData)
#         dbf.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Final Input\Input_Sams_Club_LA_PDP.csv",encoding='utf-8', index=False, sep=',')
# inpZList9 = []
# for val in finalOutput:
#     regName = val['region_name']
#     if 'SD' in str(regName):
#         InpData = str(val).replace("'", '"')
#         inpZList9.append(InpData)
#         regInpData = str(inpZList9).replace("'", '')
#         dbf = pd.read_json(regInpData)
#         dbf.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Final Input\Input_Sams_Club_SD_PDP.csv",encoding='utf-8', index=False, sep=',')
# # #========Saving Complete file in local path(whole input)==============#
# #
# fullInputfile = pd.read_json(outputInp)
# fullInputfile.to_csv(r"C:\Users\santhosh.s3\Downloads\Automation\Final Input\Input_Sams_Club_All_Regions_PDP.csv", encoding='utf-8',index=False, sep=',')


