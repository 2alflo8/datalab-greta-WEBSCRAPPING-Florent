#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 11:02:55 2019

@author: arnaudhub
"""

#from urllib3 import urlopen
import requests, json, sys, re, os, configparser
from urllib.request import urlopen
import bs4 as BeautifulSoup
import pandas as pd
import datetime
from urllib import parse
import argparse
from sqlalchemy import create_engine
from sqlalchemy.sql import text

BASE = 'https://candidat.pole-emploi.fr'
###compteur###
debut = datetime.datetime.now()
print(debut)

##################creer l'engine               #############"
config = configparser.ConfigParser()
config.read_file(open(os.path.expanduser("/home/arnaudhub/Documents/PROJET3/datalab.cnf")))

DB = "bdd_popol?charset=utf8"
TBL = "jobbot"
CNF = "myBDD"

engine = create_engine("mysql://%s:%s@%s/%s" % (config[CNF]['user'], parse.quote_plus(config[CNF]['password']), config[CNF]['host'], DB))

#sql_commande = engine.execute("""CREATE TABLE IF NOT EXISTS `bdd_popol`.jobbotv2 (
#    reference VARCHAR (15) NOT NULL,
#    intitule VARCHAR (250) NULL ,
#    Date_du_jour VARCHAR (110) NULL,
#    date_actualise VARCHAR (110) NULL,
#    lien_offre VARCHAR (250) NULL,
#    type_ VARCHAR (250) NULL,
#    contrat VARCHAR (250) NULL,
#    duree VARCHAR (250) NULL,
#    temps_hebdo VARCHAR (250) NULL,
#    dept_ville VARCHAR (250) NULL,
#    salaire VARCHAR (250) NULL,
#    site_recruteur VARCHAR (250) NULL,
#    contact_mail VARCHAR (250)  NULL,
#    contact_personne VARCHAR (250) NULL,
#    experience VARCHAR (250)  NULL,
#    id_partenaire VARCHAR (250) NULL,
#    secteur_activite VARCHAR (250) NULL,
#    PRIMARY KEY (`reference`))
#ENGINE = InnoDB
#AUTO_INCREMENT = 0
#DEFAULT CHARACTER SET = utf8
#COLLATE = utf8_bin;
#""")


########creation de liste de recherche lieux et metier######

liste_lieux=["24R","53R"]
liste_metier=["data","developpeur","python"]
lieux = liste_lieux
motscles = liste_metier
#URL= BASE+'/offres/recherche?lieux=24R&motsCles=donn%C3%A9es&offresPartenaires=true&range=0-9&rayon=10&tri=0'
URL = 'https://candidat.pole-emploi.fr/offres/recherche?lieux={}&motscles={}&offresPartenaires=true&range=0-9&rayon=10&tri=0'.format(lieux,motscles)
req = requests.get(URL)
soup = BeautifulSoup.BeautifulSoup(req.text, "lxml")
#######creation data frame de collecte########

df_collecte= pd.DataFrame()
df_collecte = pd.DataFrame(columns =["reference","intitule","Date_du_jour","date_actualise","lien_offre","type_","contrat","duree","temps_hebdo",
                                   "dept_ville","salaire","site_recruteur",
                                   "contact_mail","contact_personne","experience",
                                   "id_partenaire","secteur_activite"])

######




######

i=1


########double boucle pour implementer les criteres des listes ######

for l in range(len(liste_lieux)):
    lieux=liste_lieux[l]
    l+=1
    for m in range(len(liste_metier)):
        #print(liste_metier)
        mot_cle=liste_metier[m]
        #print(mot_cle)
        html='https://candidat.pole-emploi.fr/offres/recherche?lieux={}&motsCles={}&offresPartenaires=true&range=0-9&rayon=10&tri=0'.format(lieux,mot_cle)
        print("adresse >>> ",html)

########sortir tous les url des offres si + de 10 par page ##########

        html = urlopen(html).read()
        html = BeautifulSoup.BeautifulSoup(html,features = "lxml")

########extraction du nb offres #########

        nb_offre=html.find_all("h1", class_="title")
        #print(">>>",nb_offre)23	CDI - Developpeur Web Fullstack - Solution de recrutement - Montreuil (H/F)	https://candidat.pole-emploi.fr/offres/recherche/detail/1040816	Contrat travail	Contrat à duree indeterminee			28 - MONTREUIL			2019-09-09	1040816			Experience exigee	http://joboolo.com/emploi-cdi-d%C3%A9veloppeur-web-fullstack-solution-de-recrutement-montreuil/3/48811486?&amp;utm_source=poleemploi&amp;utm_medium=classic&amp;utm_campaign=pe

        nb_offre=nb_offre[0].text
        #print(">>>>",nb_offre)
        nb_offre=re.findall(r'\d+',nb_offre)
        nb_offre=int(nb_offre[0])
        #print("nb_offre >>>> ",nb_offre,type(nb_offre))

        if nb_offre<=10:
            offre="{}-{}".format(0,nb_offre)
            html='https://candidat.pole-emploi.fr/offres/recherche?lieux={}&motsCles={}&offresPartenaires=true&range={}&rayon=10&tri=0'.format(lieux,mot_cle,offre)
            print(html)
        else:
            z=nb_offre//100

            for tour in range(z+1):
                #print("tour >>> ",tour)
                offre="{}-{}".format(((tour+1)*100)-100,(tour+1)*100)
                if tour==z:
                    offre="{}-{}".format(((tour+1)*100)-100,nb_offre)
                html='https://candidat.pole-emploi.fr/offres/recherche?lieux={}&motsCles={}&offresPartenaires=true&range={}&rayon=10&tri=0'.format(lieux,mot_cle,offre)
#                print(html)

        ########creation de la soupe ##########

                html = urlopen(html).read()
                soup = BeautifulSoup.BeautifulSoup(html,features = "lxml")
                #print(soup)
                nb_offres=0

                for adresse_offre in soup.find_all("h2",attrs={"class":"media-heading"}):
                    #print("adresse_annonce >>>" ,adresse_offre)
                    nb_offres+=1
                    #print("OFFRE BRUT >>>> ",adresse_offre)

        #######extraction de l'intitule#######

                    intitule=adresse_offre.text.replace('\n', '')

        #########extraction de l'url de l'offre########

                    #___________version regex______
                    #print("INTITULE >>>> ",intitule)
                    #ad=re. findall('(\/[\w]+\/[\w]+\/[\w]+\/[\w]+)',str(adresse_offre))
                    #offre_emploi='https://candidat.pole-emploi.fr{}'.format(ad[0])
                    #print("URL offre >>> ",offre_emploi)

                    lien_offre="https://candidat.pole-emploi.fr{}".format(adresse_offre.find('a', class_ ='btn-reset')['href'])
                    #print("LIEN OFFRE >>> ",lien_offre)

                    lien=lien_offre

        ############soupe de l'annonce########

                    lien = urlopen(lien).read()
                    soup = BeautifulSoup.BeautifulSoup(lien,features = "lxml")

        #####dans l'offre extraction et separation du type du contrat et de la duree #########

                    type_contrat=soup.find_all("dd")
                    type_contrat=type_contrat[0].text
                    type_contrat=type_contrat.replace('\n',"-")
                    type_contrat=type_contrat.replace('--',"/")
                    type_contrat=type_contrat.replace('-',"")
                    type_contrat=type_contrat.split("/")
                    contrat=type_contrat[0]
                    type_=type_contrat[1]

                    try:
                        contrat = contrat.split("  ")
                        duree =contrat[1]
                        contrat = str(contrat[0])
                        print(duree)
                    except:
                        duree=""
                        contrat=type_contrat[0]

                        print("contrat >>>> ",contrat ,"type_ >>>> ",type_)

        ##########dans l'offre extraction temps hebdomadaire#####

                    try:

                        a=soup.find_all("dd",itemprop="workHours")
                        #print(">>>",a)
                        temps_hebdo=a[0].text
                        #print(temps_hebdo)
                    except:
                        temps_hebdo=""
                        #print(temps_hebdo)

        ###########dans l'offre extraction du salaire########


                    try:
                        r=soup.find_all("dd")
                        liste_html=""
                        for x in r:
                            liste_html+=str(x)
                        id_salaire=(liste_html.index("Salaire"))+len("Salaire :")
                        id_dd=liste_html.index("</dd><dd><a")
                        salaire=liste_html[id_salaire:id_dd]
                        salaire=re.findall('[\w]+',salaire)
                        salaire=" ".join(salaire)
                        print("$$$$$alaire>>>>",salaire)
                    except:
                        salaire=""

        ##########dans l'offre extraction dept-ville#####

                    dept_ville=soup.find_all("span", itemprop="name")
                    print("2>>>",dept_ville)
                    dept_ville=dept_ville[0].text
                    print(">>>>",dept_ville)

        ###########dans l'offre extraction du site du recruteur#########

                    try:
                        site_recruteur=soup.find("a",class_='text-link')['href']
                        print("site recruteur>>>",site_recruteur)

                        liste_contact=site_recruteur.split("/")
                        print(liste_contact)

                        if "offres"  in liste_contact:
                            print(">>> offres")
                            site_recruteur=""


                    except:
                        site_recruteur=""

        ###########dans l'offre extraction de la date d'actualisation au format XXXX-XX-XX ######

                    date=soup.find_all("span",itemprop="datePosted")
                    print(date)
                    date_actualise=re.findall("([\d]{4}-[\d]{2}-[\d]{2})",str(date))
                    date_actualise=date_actualise[0]
                    print("date >>>> ",date_actualise)

        #########dans l'offre extraction de la reference ###########

                    reference=soup.find_all("span",itemprop="value")
                    reference=reference[0].text
                    print("reference >>>> ",reference)

        ###########extraction  des contacts personne et mail ########

                    try:
                        a=soup.find_all("div",class_="apply-block")
                        a
                        a=(a[0].text)
                        a=str(a)
                        a=a.replace("Adresse electronique","")
                        a = a.split()

                        contact=a[1:-2]
                        contact_mail=a[-1]
                        print(">>>> ",contact_mail)
                        contact_personne=" ".join( contact )
                        print(">>>> ",contact_personne)
                    except:
                        contact_mail=""
                        contact_personne=""

        ############extraction de l'experience##########

                    a=soup.find_all("span",itemprop="experienceRequirements")
                    experience=(a[0].text)
                    print("experience >>>> ",experience)

        #########extraction du secteur d'activite #######

                    try:
                        soup.find_all("span",itemprop="industry")
                        print(">>>",a)
                        secteur_activite=a[0].text
                        print(secteur_activite)
                    except:
                        secteur_activite=""

        ############extraction du site de l'employeur en bas######

                    try:
                        a=soup.find_all("div", class_="media")
                        a=(a[0].text)
                        a=str(a)
                        a = a.split()
                        id_site=a.index('internet')
                        site_recruteur=a[id_site+1]
                        #id_site=site_recruteur
                        print(id_site)
                    except:
                        #site_recruteur=""
                        id_site=""

        ############extraction du site du partenaire #######

                    try:
                        a=soup.find("ul", class_="partner-list"),["href"]
                        a=list(a)
                        a=a[0]
                        a=str(a)
                        a=a.replace("href=","href= ")
                        a=a.split()
                        id_site=a.index("href=")
                        id_partenaire=a[id_site+1]
                        id_partenaire=re.sub('["^\"","$\""]',"",id_partenaire)
                        print(id_partenaire)
                        #id_site=id_partenaire
                    except:
                        id_partenaire=""
                        #id_site:""


        #########remplissage de la data frame df_collecte#######
                    Date_du_jour = datetime.datetime.now()
                    print(Date_du_jour)
#                   Date_du_jour[0:10]
                    df_collecte.loc[i] = [reference,intitule,Date_du_jour,date_actualise,lien_offre,type_,contrat,duree,temps_hebdo,
                                   dept_ville,salaire,site_recruteur,
                                   contact_mail,contact_personne,experience,
                                   id_partenaire,secteur_activite]
                    print(df_collecte)
        #########comptage des offres#####

                    i+=1
print("nb_offres>>>>",i)
#soup.select('h1.title')
#chiffre = re.findall(r'\d+', soup.select('h1.title')[0].text)[0] # REGEX pour le 1er chiffre !
#N=int(chiffre)
#print("%d [%s]" % (N, URL))

#statement = text("""
#INSERT INTO jobbotv2(reference,intitule,Date_du_jour,date_actualise,lien_offre,type_,contrat,duree,temps_hebdo,
#                                   dept_ville,salaire,site_recruteur,
#                                   contact_mail,contact_personne,experience,
#                                   id_partenaire,secteur_activite)
#  VALUES(:reference, :intitule,:Date_du_jour, :date_actualise,:lien_offre, :type_,:contrat, :duree , :temps_hebdo, :dept_ville,:salaire,:site_recruteur,:contact_mail,:contact_personne,:experience, :id_partenaire,:secteur_activite )
#ON DUPLICATE KEY
#UPDATE
#
#"""

print(df_collecte)
csvpoleemploi = df_collecte.to_csv('pole_emploi.csv')


##    ADD COLUMN 'reference' PRIMARY KEY""";
#statement = text("""
#INSERT INTO jobbot(reference,intitule,Date_du_jour,date_actualise,lien_offre,type_,contrat,duree,temps_hebdo,
#                                   dept_ville,salaire,site_recruteur,
#                                   contact_mail,contact_personne,experience,
#                                   id_partenaire,secteur_activite)
#  VALUES(:reference, :intitule,:Date_du_jour, :date_actualise,:lien_offre, :type_,:contrat, :duree , :temps_hebdo, :dept_ville,:salaire,:site_recruteur,:contact_mail,:contact_personne,:experience, :id_partenaire,:secteur_activite)
#
#ON DUPLICATE KEY
#UPDATE
#DateLast = CURRENT_DATE()
#""")
#param = {'reference':reference,'intitule':intitule,'Date_du_jour':Date_du_jour,'date_actualise':date_actualise,'lien_offre':lien_offre,'type_':type_,'contrat':contrat,'duree':duree,'temps_hebdo':temps_hebdo,
#                                   'dept_ville':dept_ville,'salaire':salaire,'site_recruteur':site_recruteur,
#                                   'contact_mail':contact_mail,'contact_personne':contact_personne,'experience':experience,
#                                   'id_partenaire':id_partenaire,'secteur_activite':secteur_activite}
#
#engine.execute(sql_commande,param)

#csvpoleemploi.to_sql('jobbotv2',con=engine, if_exists= 'replace',index=False, chunksize=1)

fin = datetime.datetime.now()
print(fin)
delai = fin-debut
print(delai)
#print("Temps passe :",delai,"secondes")#
#df_collecte.to_sql(con=engine, name='jobbt', if_exists='append', chunksize=1)