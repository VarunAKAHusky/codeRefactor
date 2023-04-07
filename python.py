def splashdataFetch(request):
    startTimeExe = datetime.datetime.now()
    connection = get_connection(autocommit=True)
    cursor = connection.cursor()
    mainDataDict={"TableData":{"brand_level":[],"brand_group_level":[],"privendor_color_shade_level":[],"teritory":[]},"months":[],"MetaData":[]}
    metaData = []
    mainlist = []
    dataDict = {}
    mnths = []
    mnths_yr = set()
    try:
        user = request.user
        with connection.cursor() as cursor:
            sqlModeChange="SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"
            cursor.execute(sqlModeChange)
            # grpQ =f'''select GROUP_NM from users_customuser where username='{user}';'''
            # cursor.execute(sqlModeChange)
            # grp = pd.read_sql(grpQ, connection)
            for query in splash_queries:
                dataDict = {}
                first_time = datetime.datetime.now()
                #q = splash_queries[query].replace("tablename",grp['GROUP_NM'][0])
                df = pd.read_sql(splash_queries[query], connection)
                later_time = datetime.datetime.now()
                difference = later_time - first_time
                print("execution time for ...",query," is ..."+str(difference))
                df.fillna(0)
                #print("query .....",df.isnull().sum())
                #['TRDNG_MNTH_SHRT_NM', 'DPT_NM', 'GRP_NM', 'PRI_VNDR_PROD_NBR', 'ITM_CLR', 'ITM_SHADE', 'STND_TRRTRY_NM', 'Forecast', 'Actual', 'Demand_LY', 'Actual_LY', 'Forecast_vs_Actual', 'Forecast_vs_Actual_LY', 'Margin', 'ROS_Forecast', 'ROS_Actual', 'ROS_Actual_LY', 'Stores_Forecast', 'Stores_Actual', 'Stores_Actual_LY', 'MDQ', 'Store_SOH_Actual', 'Store_SOH_Actual_LY', 'Store_Cover_Actual', 'Total_Stock_Target', 'Total_Stock_Actual', 'Total_Cover_Actual', 'Inbound', 'Order_Quantity', 'Ending_Stock']
                df[['Forecast', 'Actual', 'Demand_LY', 'Actual_LY', 'Forecast_vs_Actual', 'Forecast_vs_Actual_LY', 'Margin', 'ROS_Forecast', 'ROS_Actual', 'ROS_Actual_LY', 'Stores_Forecast', 'Stores_Actual', 'Stores_Actual_LY', 'MDQ', 'Store_SOH_Actual', 'Store_SOH_Actual_LY', 'Store_Cover_Actual', 'Total_Stock_Target', 'Total_Stock_Actual', 'Total_Cover_Actual', 'Inbound', 'Order_Quantity', 'Ending_Stock']] = df[[ 'Forecast', 'Actual', 'Demand_LY', 'Actual_LY', 'Forecast_vs_Actual', 'Forecast_vs_Actual_LY', 'Margin', 'ROS_Forecast', 'ROS_Actual', 'ROS_Actual_LY', 'Stores_Forecast', 'Stores_Actual', 'Stores_Actual_LY', 'MDQ', 'Store_SOH_Actual', 'Store_SOH_Actual_LY', 'Store_Cover_Actual', 'Total_Stock_Target', 'Total_Stock_Actual', 'Total_Cover_Actual', 'Inbound', 'Order_Quantity', 'Ending_Stock']].apply(abs) 
                #df.abs()
                if query=="teritory":
                    first_time = datetime.datetime.now()
                    brnds = df['DPT_NM'].unique()
                    metaData =[] 
                    for brnd in brnds:
                        brnd_level = {"Brand_Name":brnd,
                                    "Stage_2":[]}
                        grps = df.loc[df['DPT_NM'] == brnd, 'GRP_NM'].unique()
                        for grp in grps:
                            dataReq = df.loc[(df['DPT_NM'] == brnd) & (df['GRP_NM'] == grp), ['PRI_VNDR_PROD_NBR','ITM_CLR','ITM_SHADE']].apply(lambda row: {'PRI_VNDR_PROD_NBR': row['PRI_VNDR_PROD_NBR'], 'ITM_CLR': row['ITM_CLR'], 'ITM_SHADE': row['ITM_SHADE']}, axis=1).drop_duplicates().tolist()
                            brnd_level['Stage_2'].append({"Brand_Name":brnd,"GRP_NM":grp,"Stage_3":dataReq})
                        metaData.append(brnd_level) 
                    mainDataDict.update({"MetaData":metaData})
                    later_time = datetime.datetime.now()
                    print("meta data time is .....",str(later_time-first_time))
                query_first_time = datetime.datetime.now()
                queryCnds = {
                    "brand_level":"DPT_NM==i[1]",
                    "brand_group_level":"(DPT_NM==i[1] and GRP_NM==i[2] )",
                    "privendor_color_shade_level":"(DPT_NM==i[1] and GRP_NM==i[2] and PRI_VNDR_PROD_NBR==i[3] and ITM_CLR==i[4] and ITM_SHADE==i[5])",
                    "teritory":"(DPT_NM==i[1] and GRP_NM==i[2] and PRI_VNDR_PROD_NBR==i[3] and ITM_CLR==i[4] and ITM_SHADE==i[5] and STND_TRRTRY_NM==i[6])"
                    
                }
                DPT_NM,GRP_NM,PRI_VNDR_PROD_NBR,ITM_CLR,ITM_SHADE,STND_TRRTRY_NM = (None,)*6
                for i in df.values:
                    mnths_yr.add(i[0]+" "+str(i[-1]))
                    if eval(queryCnds[query]) or len(dataDict)<0:
                        pass  
                    else:
                        if len(dataDict)>0:
                            mainDataDict['TableData'][query].append(dataDict)
                            dataDict = {}
                    dataDict.update({"DPT_NM":i[1],"GRP_NM":i[2],"PRI_VNDR_PROD_NBR":i[3],"ITM_CLR":i[4],
                                    "ITM_SHADE":i[5],"STND_TRRTRY_NM":i[6],"Forecast_"+i[0]:i[7],"Actual_"+i[0]:i[8],
                                    "Demand_LY_"+i[0]:i[9],"Actual_LY_"+i[0]:i[10],"Forecast_vs_Actual_"+i[0]:i[11],"Forecast_vs_Actual_LY_"+i[0]:i[12],
                                    "Margin_"+i[0]:i[13],"ROS_Forecast_"+i[0]:i[14],"ROS_Actual_"+i[0]:i[15],"ROS_Actual_LY_"+i[0]:i[16],
                                    "Stores_Forecast_"+i[0]:i[17],"Stores_Actual_"+i[0]:i[18],"Stores_Actual_LY_"+i[0]:i[19],"MDQ_"+i[0]:i[20],
                                    "Store_SOH_Actual_"+i[0]:i[21],"Store_SOH_Actual_LY_"+i[0]:i[22],"Store_Cover_Actual_"+i[0]:i[23],"Total_Stock_Target_"+i[0]:i[24],
                                    "Total_Stock_Actual_"+i[0]:i[25],"Total_Cover_Actual_"+i[0]:i[26],"Inbound_"+i[0]:i[27],"Ending_Stock_"+i[0]:i[28],
                                    "Order_Quantity_"+i[0]:i[29],"ITM_CLSS":i[-2]}) 
                    DPT_NM,GRP_NM,PRI_VNDR_PROD_NBR,ITM_CLR,ITM_SHADE,STND_TRRTRY_NM=i[1:7]
                mainDataDict['TableData'][query].append(dataDict)
                brnd_later_time = datetime.datetime.now()
                print("Python execution time for ...",query," is ..."+str(brnd_later_time-query_first_time))
                dates_sorted = sorted([datetime.datetime.strptime(d, '%b %Y') for d in mnths_yr])
                mnths =[]
                for mn in dates_sorted:
                    mySp = str(mn.strftime('%b %Y')).split(" ")
                    mnths.append({"month":mySp[0],"year":mySp[1]})
                mainDataDict.update({"months":mnths})
    except Exception as e:
            print(e)
            traceback.print_exc()
    finally :
        connection.close()
        endTimeExe = datetime.datetime.now()
        print("whole exe time was ",endTimeExe-startTimeExe)
        return mainDataDict
