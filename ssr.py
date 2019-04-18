#! usr/bin/env python3
# -*- coding:utf8 -*-
# author : DZG 2016-09-15

# 抓取http://www.ishadowsocks.org/ 网站的3条免费的账号信息，
# 并更新到我的ss配置文件中，达到运行一下，更新账号信息的功能

# 使用方法(目前) 将 filePath中的字符串换成本机的SS客户端配置文件的路径
# 需要的包 lxml，BeautifulSoup，请使用pip安装

import urllib.request,time,re,os,datetime
from bs4 import BeautifulSoup as bs

ISOTIMEFORMAT='%Y-%m-%d %X'
filePath = "E:/科学上网/Shadowsocks-3.3.1"

# 获取密码 返回按照 abc排序好的list
def getSSInfo():
    url = 'http://www.ishadowsocks.org/'
    webHtml = urllib.request.urlopen(url).read().decode('utf8', errors='replace')
    while webHtml == None :
        webHtml = urllib.request.urlopen(url).read().decode('utf8', errors='replace')
    webHtmlBs = bs(webHtml, 'lxml')
    accounts = webHtmlBs.find_all("div", class_="col-lg-4 text-center")[0:3]
    accountsRecords = []
    accountsPassword = []
    accountsPort = []
    accountsServer = []
    for a in accounts:
        pwd = bs(str(a), 'lxml').find_all(re.compile('^h4'))
        accountsPassword.append(pwd[2].get_text()[4:])
        accountsServer.append(pwd[0].get_text()[7:])
        accountsPort.append(pwd[1].get_text()[3:])
    accountsRecords.append(accountsPassword)
    accountsRecords.append(accountsServer)
    accountsRecords.append(accountsPort)
    return accountsRecords

#  写入密码
def fixPwd(filePath,filePathB,starttime):
    pwds = getSSInfo()[0]
    servers = getSSInfo()[1]
    ports = getSSInfo()[2]
    try:
        createFile(filePath,filePathB)
        file = open(filePath, 'r+')

        nowtime = datetime.datetime.now()
        if (nowtime - starttime).seconds > 10:
            print("这次打开网页耗时有点叼。。")
        elif (nowtime - starttime).seconds <= 1:
            print("网速起飞。。")
        else:
            print("中规中矩。。")
            
        filelists = file.readlines()

        filelists[3] = ''
        filelists[3] = '      "server": "' + servers[0] + '",\n'
        filelists[4] = ''
        filelists[4] = '      "server_port": ' + ports[0] + ',\n'
        filelists[5] = ''
        filelists[5] = '      "password": "' + pwds[0] + '",\n'

        filelists[11] = ''
        filelists[11] = '      "server": "' + servers[1] + '",\n'
        filelists[12] = ''
        filelists[12] = '      "server_port": ' + ports[1] + ',\n'
        filelists[13] = ''
        filelists[13] = '      "password": "' + pwds[1] + '",\n'

        filelists[19] = ''
        filelists[19] = '      "server": "' + servers[2] + '",\n'
        filelists[20] = ''
        filelists[20] = '      "server_port": ' + ports[2] + ',\n'
        filelists[21] = ''
        filelists[21] = '      "password": "' + pwds[2] + '",\n'

        file2 = open(filePath, 'w+')
        file2.writelines(filelists)
    finally:
        if file:
            file.close()

# 创建配置文件
def createFile(filePath,filePathB):
    try:
        #gui-config.json文件
        fileExist =  os.path.exists(filePath)
        if fileExist == False:
            open(filePath, 'a', encoding='utf-8')

        open(filePath, 'r+').truncate()
        file = open(filePath, 'r+')
        file.write('{\n')

        file.write('  "configs": [\n')
        file.write('    {\n')
        file.write('      "server": "USA.ISS.TF",\n')
        file.write('      "server_port": 23456,\n')
        file.write('      "password": "41633038",\n')
        file.write('      "method": "aes-256-cfb",\n')
        file.write('      "remarks": "USA",\n')
        file.write('      "auth": false\n')
        file.write('    },\n')

        file.write('    {\n')
        file.write('      "server": "USB.ISS.TF",\n')
        file.write('      "server_port": 443,\n')
        file.write('      "password": "21655234",\n')
        file.write('      "method": "aes-256-cfb",\n')
        file.write('      "remarks": "USA",\n')
        file.write('      "auth": false\n')
        file.write('    },\n')

        file.write('    {\n')
        file.write('      "server": "USB.ISS.TF",\n')
        file.write('      "server_port": 443,\n')
        file.write('      "password": "21655234",\n')
        file.write('      "method": "aes-256-cfb",\n')
        file.write('      "remarks": "Jp",\n')
        file.write('      "auth": false\n')
        file.write('    }\n')
        file.write('  ],\n')

        file.write('  "strategy": "com.shadowsocks.strategy.ha",\n')
        file.write('  "index": -1,\n')
        file.write('  "global": false,\n')
        file.write('  "enabled": false,\n')
        file.write('  "shareOverLan": true,\n')
        file.write('  "isDefault": false,\n')
        file.write('  "localPort": 1080,\n')
        file.write('  "pacUrl": null,\n')
        file.write('  "useOnlinePac": false,\n')
        file.write('  "availabilityStatistics": false,\n')
        file.write('  "autoCheckUpdate": true,\n')
        file.write('  "isVerboseLogging": false,\n')

        file.write('  "logViewer": {\n')
        file.write('    "fontName": "Consolas",\n')
        file.write('    "fontSize": 8.0,\n')
        file.write('    "bgColor": "black",\n')
        file.write('    "textColor": "white",\n')
        file.write('    "topMost": false,\n')
        file.write('    "wrapText": false,\n')
        file.write('    "toolbarShown": false,\n')
        file.write('    "width": 600,\n')
        file.write('    "height": 400,\n')
        file.write('    "top": 650,\n')
        file.write('    "left": 1320,\n')
        file.write('    "maximized": true\n')
        file.write('  },\n')
        
        file.write('  "proxy": {\n')
        file.write('    "useProxy": false,\n')
        file.write('    "proxyServer": "",\n')
        file.write('    "proxyPort": 0\n')
        file.write('  },\n')

        file.write('  "hotkey": {\n')
        file.write('    "SwitchSystemProxy": "",\n')
        file.write('    "ChangeToPac": "",\n')
        file.write('    "ChangeToGlobal": "",\n')
        file.write('    "SwitchAllowLan": "",\n')
        file.write('    "ShowLogs": "",\n')      
        file.write('    "ServerMoveUp": "",\n')
        file.write('    "ServerMoveDown": ""\n')
        file.write('  }\n')

        file.write('}\n')
        file.write('//UpdateTime : ' + time.strftime(ISOTIMEFORMAT, time.localtime()))


        #statistics-config.json文件
        fileExistB =  os.path.exists(filePathB)
        if fileExistB == False:
            open(filePathB, 'a', encoding='utf-8')

        open(filePathB, 'r+').truncate()
        fileB = open(filePathB, 'r+')

        fileB.write('{\n')
        fileB.write('  "Calculations": {\n')
        fileB.write('    "AverageLatency": 0.0,\n')
        fileB.write('    "MinLatency": 0.0,\n')
        fileB.write('    "MaxLatency": 0.0,\n')
        fileB.write('    "AverageInboundSpeed": 0.0,\n')
        fileB.write('    "MinInboundSpeed": 0.0,\n')
        fileB.write('    "MaxInboundSpeed": 0.0,\n')
        fileB.write('    "AverageOutboundSpeed": 0.0,\n')
        fileB.write('    "MinOutboundSpeed": 0.0,\n')
        fileB.write('    "MaxOutboundSpeed": 0.0,\n')
        fileB.write('    "AverageResponse": 0.0,\n')
        fileB.write('    "MinResponse": 0.0,\n')
        fileB.write('    "MaxResponse": 0.0,\n')
        fileB.write('    "PackageLoss": 0.0\n')
        fileB.write('  },\n')

        fileB.write('  "StatisticsEnabled": false,\n')
        fileB.write('  "ByHourOfDay": true,\n')
        fileB.write('  "Ping": false,\n')
        fileB.write('  "ChoiceKeptMinutes": 10,\n')
        fileB.write('  "DataCollectionMinutes": 10,\n')
        fileB.write('  "RepeatTimesNum": 4\n')
        fileB.write('}\n')

        fileB.write('//UpdateTime : ' + time.strftime(ISOTIMEFORMAT, time.localtime()))
    finally:
        if file:
            file.close()
        if fileB:
            fileB.close()



if __name__ == '__main__':
    print('开始：', time.strftime(ISOTIMEFORMAT, time.localtime()))
    starttime = datetime.datetime.now()
    filePathA = filePath + "/gui-config.json"
    filePathB = filePath + "/statistics-config.json"
    fixPwd(filePathA,filePathB,starttime)
    print('结束：', time.strftime(ISOTIMEFORMAT, time.localtime()))
