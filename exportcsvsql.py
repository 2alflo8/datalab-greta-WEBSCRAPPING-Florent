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

config = configparser.ConfigParser()
config.read_file(open(os.path.expanduser("/home/arnaudhub/Documents/PROJET3/datalab.cnf")))

DB = "bdd_popol?charset=utf8"
TBL = "jobbot"
CNF = "myBDD"

engine = create_engine("mysql://%s:%s@%s/%s" % (config[CNF]['user'], parse.quote_plus(config[CNF]['password']), config[CNF]['host'], DB))


csvpoleemploi = pd.read_csv('pole_emploi.csv')
csvpoleemploi = csvpoleemploi[["reference","intitule","Date_du_jour","date_actualise","lien_offre","type_","contrat","duree","temps_hebdo","dept_ville","salaire","site_recruteur","contact_mail","contact_personne","experience","id_partenaire","secteur_activite"]]

#poleemploi.to_sql('jobbot',con=engine, if_exists= 'append',index=False, chunksize=1)

csvpoleemploi = csvpoleemploi.drop_duplicates(['reference'],keep='last')

sql_commande = engine.execute("""CREATE TABLE IF NOT EXISTS `bdd_popol`.TBL (
    reference VARCHAR (15) NOT NULL,
    intitule VARCHAR (250) NULL ,
    Date_du_jour VARCHAR (110) NULL,
    date_actualise VARCHAR (110) NULL,
    lien_offre VARCHAR (250) NULL,
    type_ VARCHAR (250) NULL,
    contrat VARCHAR (250) NULL,
    duree VARCHAR (250) NULL,
    temps_hebdo VARCHAR (250) NULL,
    dept_ville VARCHAR (250) NULL,
    salaire VARCHAR (250) NULL,
    site_recruteur VARCHAR (250) NULL,
    contact_mail VARCHAR (250)  NULL,
    contact_personne VARCHAR (250) NULL,
    experience VARCHAR (250)  NULL,
    id_partenaire VARCHAR (250) NULL,
    secteur_activite VARCHAR (250) NULL,
    PRIMARY KEY (`reference`))
ENGINE = InnoDB
AUTO_INCREMENT = 0
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;
""")


#    ADD COLUMN 'reference' PRIMARY KEY""";
statement = text("""
INSERT INTO jobbot(reference,intitule,Date_du_jour,date_actualise,lien_offre,type_,contrat,duree,temps_hebdo,
                                   dept_ville,salaire,site_recruteur,
                                   contact_mail,contact_personne,experience,
                                   id_partenaire,secteur_activite)
  VALUES(:reference, :intitule,:Date_du_jour, :date_actualise,:lien_offre, :type_,:contrat, :duree , :temps_hebdo, :dept_ville,:salaire,:site_recruteur,:contact_mail,:contact_personne,:experience, :id_partenaire,:secteur_activite)

ON DUPLICATE KEY
UPDATE
DateLast = CURRENT_DATE()
""")
param = {'reference':reference,'intitule':intitule,'Date_du_jour':Date_du_jour,'date_actualise':date_actualise,'lien_offre':lien_offre,'type_':type_,'contrat':contrat,'duree':duree,'temps_hebdo':temps_hebdo,
                                   'dept_ville':dept_ville,'salaire':salaire,'site_recruteur':site_recruteur,
                                   'contact_mail':contact_mail,'contact_personne':contact_personne,'experience':experience,
                                   'id_partenaire':id_partenaire,'secteur_activite':secteur_activite}

engine.execute(sql_commande,param)



csvpoleemploi.to_sql('jobbotv2',con=engine, if_exists= 'replace',index=False, chunksize=1)
