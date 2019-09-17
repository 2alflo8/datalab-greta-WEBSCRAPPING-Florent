#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 20:14:36 2019

@author: rousseau
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 11:44:47 2019

@author: arnaudhub
"""


import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import configparser,os
from urllib import parse

##Creation de liste
reference=[]
intitule=[]
Date_du_jour=[]
date_actualise=[]
lien_offre=[]
type_=[]
contrat=[]
duree=[]
temps_hebdo=[]
dept_ville=[]
salaire=[]
site_recruteur=[]
contact_mail=[]
contact_personne=[]
experience=[]
id_partenaire=[]
secteur_activite=[]


config = configparser.ConfigParser()
config.read_file(open(os.path.expanduser("/home/rousseau/datalab.cnf")))

DB = "bdd_popol?charset=utf8"
TBL = "jobv27"
CNF="myBDD"

mySQLengine = create_engine("mysql://%s:%s@%s/%s" % (config[CNF]['user'], parse.quote_plus(config[CNF]['password']), config[CNF]['host'], DB))
print(mySQLengine)



#poleemploi.to_sql('jobbot',con=engine, if_exists= 'append',index=False, chunksize=1)


mySQLengine.execute ("""
CREATE TABLE IF NOT EXISTS `bdd_popol`.`jobv27` (
    reference VARCHAR(15) NOT NULL,
    intitule VARCHAR(250) NULL DEFAULT NULL,
    Date_du_jour DATE,
    date_actualise DATE,
    lien_offre VARCHAR(250) NULL DEFAULT NULL,
    type_ VARCHAR(250) NULL DEFAULT NULL,
    contrat VARCHAR(250) NULL DEFAULT NULL,
    duree VARCHAR(250) NULL DEFAULT NULL,
    temps_hebdo VARCHAR(250) NULL DEFAULT NULL,
    dept_ville VARCHAR(250) NULL DEFAULT NULL,
    salaire VARCHAR(250) NULL DEFAULT NULL,
    site_recruteur VARCHAR(250) NULL DEFAULT NULL,
    contact_mail VARCHAR(250)  NULL DEFAULT NULL, 
    contact_personne VARCHAR(250) NULL DEFAULT NULL,
    experience VARCHAR(250)  NULL DEFAULT NULL,
    id_partenaire VARCHAR(250) NULL DEFAULT NULL,
    secteur_activite VARCHAR(250) NULL,
   PRIMARY KEY (`reference`))    
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;
""")
#i=0
#localite = list_columns
#for loop in range(len(localite)):
#    ligne = df_collecte[i,:]
#    i+=1

#statement = text("""
#INSERT INTO jobv26(reference,intitule,Date_du_jour,date_actualise,lien_offre,type_,contrat,duree,temps_hebdo,
#                                   dept_ville,salaire,site_recruteur,
#                                   contact_mail,contact_personne,experience,
#                                   id_partenaire,secteur_activite)
#    VALUES(:reference, :intitule,:Date_du_jour, :date_actualise,:lien_offre, :type_,:contrat, :duree , :temps_hebdo, :dept_ville,:salaire,:site_recruteur,:contact_mail,:contact_personne,:experience, :id_partenaire,:secteur_activite)
#
#    ON DUPLICATE KEY
#    UPDATE
#    date_actualise = CURRENT_DATE()
#""")
#param = {'reference':reference,'intitule':intitule,'lien_offre':lien_offre,'type_':type_,'contrat':contrat,'duree':duree,'temps_hebdo':temps_hebdo,
#                                   'dept_ville':dept_ville,'salaire':salaire,'site_recruteur':site_recruteur,
#                                   'contact_mail':contact_mail,'contact_personne':contact_personne,'experience':experience,
#                                   'id_partenaire':id_partenaire,'secteur_activite':secteur_activite}
#
#mySQLengine.execute(statement,param)



csvpoleemploicsv = pd.read_csv("/home/rousseau/pole_emploi2.csv", encoding = 'utf8', sep=',')
csvpoleemploicsv.columns = ("","reference","intitule","Date_du_jour","date_actualise","lien_offre","type_","contrat","duree","temps_hebdo","dept_ville","salaire","site_recruteur","contact_mail","contact_personne","experience","id_partenaire","secteur_activite")

csvpoleemploi = csvpoleemploicsv[["reference","intitule","Date_du_jour","date_actualise","lien_offre","type_","contrat","duree","temps_hebdo","dept_ville","salaire","site_recruteur","contact_mail","contact_personne","experience","id_partenaire","secteur_activite"]]
print(csvpoleemploi)

#csvpoleemploi = pd.read_csv("/home/rousseau/pole_emploi.csv")
#csvpoleemploi = csvpoleemploi[["reference","intitule","Date_du_jour","date_actualise","lien_offre","type_","contrat","duree","temps_hebdo","dept_ville","salaire","site_recruteur","contact_mail","contact_personne","experience","id_partenaire","secteur_activite"]]

csvpoleemploi = csvpoleemploi.drop_duplicates(['reference'],keep='last')


#csvpoleemploi.to_sql('jobbotv2',con=mySQLengine, if_exists= 'replace',index=False, chunksize=1)                                  

#~ mySQLengine.execute("TRUNCATE jobbotv25;")
csvpoleemploi.to_sql('jobv27', mySQLengine, if_exists= 'append',index=False, chunksize=1)  

