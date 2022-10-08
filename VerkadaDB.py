#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import requests
import uuid
#Import any libraries you need

## Do not edit
class VerkadaDB():
    def __init__(self):
        self._data = {}
     ## You may add to the class definition below this line
        
## To-do: add class methods
    def addTable(self, tableName):
        if tableName in self._data.keys():
            return print("error: Table already exists")
        else:
            self._data[tableName] = {}
            
    def addRow(self, tableName, rowData):
        self._data[tableName][str(uuid.uuid4())] = rowData
    
    def getRows(self,tableName, matchingCriteria):
        
        #matchingCriteria format = { "db" : "dbInstance",
        #                            "header1" : "name",
        #                            "operator1" : "==",
        #                            "value1 : "kyle",
        #                            "logOp1" : "&",
        #                            "element2" : "age",
        #                            "operator2" : "==",
        #                            "value2 : "25",
        #                            "logOp2" : "&",
        #                            .
        #                            .
        #                            .}
        
        var = int(list(matchingCriteria.keys())[-1][-1])
        d={}
        i=1
        while i<=var:
            if i!=var:
                if type(matchingCriteria["value"+str(i)]) is str:
                    tempString = f'(v["{matchingCriteria["header"+str(i)]}"] {matchingCriteria["operator"+str(i)]} "{matchingCriteria["value"+str(i)]}") {matchingCriteria["logOp"+str(i)]} '
                else:
                    tempString = f'(v["{matchingCriteria["header"+str(i)]}"] {matchingCriteria["operator"+str(i)]} {matchingCriteria["value"+str(i)]}) {matchingCriteria["logOp"+str(i)]} '
                d[i] = tempString
            else:
                if type(matchingCriteria["value"+str(i)]) is str:
                    tempString = f'(v["{matchingCriteria["header"+str(i)]}"] {matchingCriteria["operator"+str(i)]} "{matchingCriteria["value"+str(i)]}")'
                else:
                    tempString = f'(v["{matchingCriteria["header"+str(i)]}"] {matchingCriteria["operator"+str(i)]} {matchingCriteria["value"+str(i)]})'
                d[i] = tempString
            i+=1
    
        funcStr = '{k:v for (k,v) in %s._data["%s"].items() if ' % (matchingCriteria["db"], tableName)
        
        for x in d.values():
            funcStr += x
        funcStr += "}"
        
        return eval(funcStr)
    
    def updateRows(self, tableName, matchingCriteria, updateInformation):
        
        #updateInformation Format = {"name" : "john", "age" : 20}
        
        cur=self.getRows(tableName, matchingCriteria)
        
        for x in cur.values():
            i=0
            while i<len(updateInformation.keys()):
                x[list(updateInformation.keys())[i]] = list(updateInformation.values())[i]
                i+=1
    
    def deleteRows(self,tableName, matchingCriteria):
        
        cur=self.getRows(tableName, matchingCriteria)
        
        for x in cur.keys():
            self._data[tableName].pop(x)

## Do not edit   
dbInstance = VerkadaDB()

## To-do: Implement Function (mimics AWS Lambda handler)
## Input: JSON String which mimics AWS Lambda input
def lambda_handler(json_input):
    global dbInstance
    
    dict1 = json.loads(json_input)
    lead = {}
    
    lead["name"] = dict1["email"].split("@")[0]
    name = lead["name"]
    lead["email"] = dict1["email"]
    lead["domain"] = dict1["email"].split("@")[1].split(".")[0]
    lead["topLevelName"] = dict1["email"].split("@")[1].split(".")[1]
    
    #agify
    response = requests.get(f"https://api.agify.io?name={name}")
    data = response.text
    parsed= json.loads(data)
    
    age = parsed["age"]
    lead["age"] = age
    
    #genderize
    response = requests.get(f"https://api.genderize.io?name={name}")
    data = response.text
    parsed= json.loads(data)
    
    gender = parsed["gender"]
    lead["gender"] = gender
    
    #nationalize
    response = requests.get(f"https://api.nationalize.io?name={name}")
    data = response.text
    parsed= json.loads(data)
    maxProb = max(parsed['country'], key=lambda p: p['probability'])
    
    nat = maxProb["country_id"]
    lead["nationality"] = nat
    
    if lead["domain"].upper() != "VERKADA":
        dbInstance.addRow("possibleLeads",lead)
    
    json_output = lead #json.dumps({})
    #return json_output

dbInstance.addTable("possibleLeads")


## Do not edit
lambda_handler(json.dumps({"email":"John@acompany.com"}))
lambda_handler(json.dumps({"email":"Willy@bcompany.org"}))
lambda_handler(json.dumps({"email":"Kyle@ccompany.com"}))
lambda_handler(json.dumps({"email":"Georgie@dcompany.net"}))
lambda_handler(json.dumps({"email":"Karen@eschool.edu"}))
lambda_handler(json.dumps({"email":"Annie@usa.gov"}))
lambda_handler(json.dumps({"email":"Elvira@fcompay.org"}))
lambda_handler(json.dumps({"email":"Juan@gschool.edu"}))
lambda_handler(json.dumps({"email":"Julie@hcompany.com"}))
lambda_handler(json.dumps({"email":"Pierre@ischool.edu"}))
lambda_handler(json.dumps({"email":"Ellen@canada.gov"}))
lambda_handler(json.dumps({"email":"Craig@jcompany.org"}))
lambda_handler(json.dumps({"email":"Juan@kcompany.net"}))
lambda_handler(json.dumps({"email":"Jack@verkada.com"}))
lambda_handler(json.dumps({"email":"Jason@verkada.com"}))
lambda_handler(json.dumps({"email":"Billy@verkada.com"}))
lambda_handler(json.dumps({"email":"Brent@verkada.com"}))

matchingCriteria = {"db": "dbInstance", "header1": "name", "operator1" : "==", "value1" : "Kyle"}
updateInformation = {"age": 26}

dbInstance.updateRows("possibleLeads", matchingCriteria, updateInformation)

matchingCriteria = {"db": "dbInstance", "header1": "name", "operator1" : "==", "value1" : "Craig"}

dbInstance.deleteRows("possibleLeads", matchingCriteria)

matchingCriteria = { "db":"dbInstance",
                    "header1" : "age", 
                    "operator1" : ">=", 
                    "value1": 30,
                   "logOp1" : "&",
                   "header2" : "gender",
                   "operator2" : "==",
                   "value2" : "male"}

resp = dbInstance.getRows("possibleLeads", matchingCriteria)
res = dict(sorted(resp.items(), key = lambda x: x[1]['age'])[:4])

req ={}
req["name"] = "Tariq Al Akkad"
req["queryData"] = res
req["databaseContents"] = dbInstance._data

r = requests.post("https://rwph529xx9.execute-api.us-west-1.amazonaws.com/prod/pushToSlack", json=req)

