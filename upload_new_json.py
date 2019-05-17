# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 14:42:00 2019

@author: Corentin BALLAZ
"""
import csv
import mysql.connector

class TourismInDB:
    
    def __init__(self,name):
        self.initDB()
        self.name = name
        
    def __str__(self):
        return self.name

    def initDB(self):
        self.mydb = mysql.connector.connect(host="localhost", user="root", passwd="", database="dataTourism")
        self.mycursor = self.mydb.cursor()

    def createTable(self):
        try:
            self.mycursor.execute("""DROP TABLE Tourism""")
        except:
            print("Création d'une nouvelle table")
        self.mycursor.execute("""CREATE TABLE IF NOT EXISTS Tourism (
                id integer auto_increment primary key,
                nom varchar(100) DEFAULT NULL,
                description varchar(500) DEFAULT NULL,
                ville varchar(100) DEFAULT NULL,
                codePostal varchar(6) DEFAULT NULL,
                adresse varchar(200) DEFAULT NULL,
                telephone varchar(100) DEFAULT NULL,
                email varchar(200) DEFAULT NULL,
                homepage varchar(400) DEFAULT NULL
                )""")
        self.mydb.commit()

    def deleteTable(self):
        try:
            self.mycursor.execute("""DROP TABLE Tourism""")
        except:
            print("error : table doesn't exists")

    def viderTable(self):
        try:
            self.mycursor.execute("""TRUNCATE TABLE Tourism""")
        except:
            print("error : table is already empty")

    def insertIntoTable(self,tableOfDictionnary):
        i = 0
        for dico in tableOfDictionnary:
            try:
                i = i + 1
                self.mycursor.execute(
                    """INSERT INTO Tourism (nom, description, ville, codePostal, adresse, telephone, email, homepage)VALUES(%(name)s ,%(description)s ,%(ville)s ,%(codePostal)s ,%(adresse)s ,%(telephone)s ,%(email)s ,%(homepage)s)""",
                    dico)
                self.mydb.commit()
            except:                
                continue


def recupNameComment(file):
    listeNameComment = []
    listeLiaison = []
    listeAdresse = []
    with open(file, newline='',encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        print("fichier ouvert")
        print("...lecture du fichier en cours...")
        for row in reader:            
            if ('comment' in row['predicate']) and ('/13/' in row['subject']):
                dico = {}
                dico["subject1"] = row['subject']
                dico["description"] = row['object']
                listeNameComment.append(dico)
            elif ('label' in row['predicate']) and ('/13/' in row['subject']):
                for dico in listeNameComment :
                    if (dico["subject1"]==row['subject']):
                        dico["name"]=row['object']
            elif('#isLocatedAt' in row['predicate']) and ('/13/' in row['subject']):
                 for dico in listeNameComment :
                    if (dico["subject1"]==row['subject']):
                        dico["adresseLiaison1"]=row['object']
            elif('#hasContact' in row['predicate']) and ('/13/' in row['subject']):
                 for dico in listeNameComment :
                    if (dico["subject1"]==row['subject']):
                        dico["adresseLiaison1Contact"]=row['object']
            elif('schema.org/address' in row['predicate']) and not('schema.org/addressLocality' in row['predicate']):
                trouve = False
                for dico2 in listeLiaison :
                    if (dico2["adresseLiaison1"]==row['subject']):
                        dico2["adresseLiaison2"]=row['object']
                        trouve = True
                if not(trouve):
                    dico2 = {}
                    dico2["adresseLiaison1"]=row['subject']
                    dico2["adresseLiaison2"]=row['object']
                    listeLiaison.append(dico2)
            elif('schema.org/email' in row['predicate']):
                trouve = False
                for dico2 in listeLiaison :
                    if (dico2["adresseLiaison1"]==row['subject']):
                        dico2["email"]=row['object']
                        trouve = True
                if not(trouve):
                    dico2 = {}
                    dico2["adresseLiaison1"]=row['subject']
                    dico2["email"]=row['object']
                    listeLiaison.append(dico2)                    
            elif('schema.org/telephone' in row['predicate']):
                trouve = False
                for dico2 in listeLiaison :
                    if (dico2["adresseLiaison1"]==row['subject']):
                        dico2["telephone"]=row['object']
                        trouve = True
                if not(trouve):
                    dico2 = {}
                    dico2["adresseLiaison1"]=row['subject']
                    dico2["telephone"]=row['object']
                    listeLiaison.append(dico2)
            elif('homepage' in row['predicate']):
                trouve = False
                for dico2 in listeLiaison :
                    if (dico2["adresseLiaison1"]==row['subject']):
                        dico2["homepage"]=row['object']
                        trouve = True
                if not(trouve):
                    dico2 = {}
                    dico2["adresseLiaison1"]=row['subject']
                    dico2["homepage"]=row['object']
                    listeLiaison.append(dico2)
            elif('schema.org/addressLocality' in row['predicate']):
                dico3 = {}
                dico3["adresseLiaison2"]=row['subject']
                dico3["ville"]=row['object']
                listeAdresse.append(dico3)
            elif('schema.org/postalCode' in row['predicate']):
                for dico3 in listeAdresse :
                    if (dico3["adresseLiaison2"]==row['subject']):
                        dico3["codePostal"]=row['object']
            elif('schema.org/streetAddress' in row['predicate']):
                for dico3 in listeAdresse :
                    if (dico3["adresseLiaison2"]==row['subject']):
                        dico3["adresse"]=row['object']
    print("lecture du fichier terminée")
    print("...lecture et écriture des informations géographiques...")
            
    #traitement des dictionnaires
    for dico3 in listeAdresse:
        for dico2 in listeLiaison:
            if "adresseLiaison2" in dico2:
                if dico3["adresseLiaison2"]==dico2["adresseLiaison2"]:
                    dico2["ville"]=dico3["ville"]
                    dico2["codePostal"]=dico3["codePostal"]
                    try :
                        dico2["adresse"]=dico3["adresse"]
                    except:
                        dico2["adresse"]=None
    print("lecture et écriture des informations géographiques terminées")
    print("...lecture et écriture des informations de contacts...")
                
    for dico in listeNameComment:
        for dico2 in listeLiaison:
            if dico["adresseLiaison1"]==dico2["adresseLiaison1"]:
                dico["ville"]=dico2["ville"]
                dico["codePostal"]=dico2["codePostal"]
                dico["adresse"]=dico2["adresse"]
            if "adresseLiaison1Contact" in dico:
                if dico["adresseLiaison1Contact"]==dico2["adresseLiaison1"]:
                    try:
                        dico["telephone"]=dico2["telephone"]
                    except:
                        dico["telephone"]=None
                    try:
                        dico["email"]=dico2["email"]
                    except:
                        dico["email"]=None
                    try:
                        dico["homepage"]=dico2["homepage"]
                    except:
                        dico["homepage"]=None

    print("lecture et écriture des informations de contacts terminés")
                
    return (listeNameComment)
                            
    
    
myFile = 'petit.csv'
myList = recupNameComment(myFile)
print("...insertion dans la base de données en cours...")
database = TourismInDB('tourisme Savoie et Haute-Savoie')
database.createTable()
database.insertIntoTable(myList)
print("données insérée dans la base de données")


     
    



    