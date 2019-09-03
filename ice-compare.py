#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" ice-compare.py

A tool built using the Python ICE Client (pyiceclient) to compare the output of ICE to another immunization forecaster.

See README.md for info.

"""

import json
import datetime
import sys
import cx_Oracle
import pyiceclient
import uuid
import configparser
from collections import defaultdict, OrderedDict

#
# ICE vaccine group name to registry vaccine group ID mapping (VG) and reverse mapping (VGBC)
#

VG={}
VG['Hep B Vaccine Group'] = 1
VG['Rotavirus Vaccine Group'] = 14
VG['DTP Vaccine Group'] = 2
VG['Hib Vaccine Group'] = 3
VG['Pneumococcal Vaccine Group'] = 9
VG['Polio Vaccine Group'] = 4
VG['MMR Vaccine Group'] = 5
VG['Varicella Vaccine Group'] = 6
VG['Hep A Vaccine Group'] = 10
VG['Meningococcal Vaccine Group'] = 12
VG['HPV Vaccine Group'] = 15
VG['Meningococcal B Vaccine Group'] = 17
VG['Influenza Vaccine Group'] = 11
VG['H1N1 Influenza Vaccine Group'] = 16
# registry vaccine group 8 is "Other" and not mapped

VGBC={}

for vgname in VG:
    VGBC[str(VG[vgname])] = vgname

#
# Load configuration data from ice-compare.ini
#

config = configparser.ConfigParser()
config.read('ice-compare.ini')

#
# Other globals
#

DEBUG = False


#
# global database connection
#

ORA_CON = cx_Oracle.connect(config['database']['username'], config['database']['password'], config['database']['sid'])
ORA_CUR = ORA_CON.cursor()


#
# store a dict of registry evaluation code to code descriptions in EVAL{}
#

ORA_CUR.execute("""
     SELECT  evaluation_code,code_desc from algorithm_evaluation_code  """)

EVAL = {}
for evaluation_code,code_desc in ORA_CUR:
    EVAL[evaluation_code] = code_desc

#
# Main patient query: Load number_of_children random children (all less than 19 years old) in to child_list array
#

ORA_CUR.execute("""
     SELECT *
     FROM   (
         SELECT child_id,decode(sex,'M','M','F') as sex,TO_CHAR(birth_date_time,'YYYYMMDD') AS dob
         FROM   child
         WHERE  months_between(sysdate,birth_date_time) < 228
         ORDER BY DBMS_RANDOM.VALUE  )
     WHERE  rownum <= """ + config['compare']['number_of_children'])
    
child_list = []
for child_id,sex,dob in ORA_CUR:
    child_list.append((child_id,sex,dob))

#
# Main loop: For each child in the child_list array, get the IZ data, send to ICE
#

for child_id,sex,dob in child_list:
    cid=child_id
    ORA_CUR.execute("""
    SELECT
        i.child_id, TO_CHAR(i.date_of_admin,'YYYYMMDD') AS date_of_admin, i.cpt_code as cvx_code
    FROM
        immunization i
    WHERE
        i.child_id = :child_id
    ORDER BY
        i.date_of_admin""", {"child_id":child_id})

    #
    # build data structure to send child to ICE
    #

    data_dict = defaultdict(dict)
    data_list = []

    data_dict['id'] = 'patient 0'
    data_dict['firstName'] = 'First'
    data_dict['lastName'] = 'Last'
    data_dict['gender'] = sex
    data_dict['dob'] = dob
    data_dict['evalDate'] = datetime.date.today().strftime('%Y%m%d')
    data_dict['izs'] = []

    iz_count = 0
    for child_id,date_of_admin,cvx_code in ORA_CUR:
        izs = [str(uuid.uuid4()), date_of_admin, cvx_code, 'I']
        data_dict['izs'].append(izs)
        data_list.append(data_dict)
        iz_count += 1

    if iz_count == 0:
        continue

    #
    # call ICE
    #

    request_vmr = pyiceclient.data2vmr(data_list)
    response_vmr = pyiceclient.send_request(request_vmr, config['ice']['service_endpoint'], datetime.date.today().strftime('%Y-%m-%d'))
    if DEBUG:
        print (response_vmr)
    (evaluation_list, recommendation_list) = pyiceclient.process_vmr(response_vmr)

    #
    # first, compare the ICE evaluations to evaluations stored in the registry
    #
    evaluations = {}
    for evaluation in evaluation_list:
        cvx = evaluation[pyiceclient.ICE_EVALS_VACCINE].split(':')[0]
        date_of_admin = evaluation[pyiceclient.ICE_EVALS_DATE_OF_ADMIN]
        if evaluation[pyiceclient.ICE_EVALS_GROUP] not in VG:
            continue
        key=date_of_admin+"|"+cvx+"|"+str(VG[evaluation[pyiceclient.ICE_EVALS_GROUP]])
        evaluations[key] = evaluation[pyiceclient.ICE_EVALS_EVAL_CODE] + "^" + evaluation[pyiceclient.ICE_EVALS_EVAL_INTERP]

    ORA_CUR.execute("""
        SELECT cpt_code AS cvx,
            TO_CHAR(date_of_admin,'YYYYMMDD') AS date_of_admin,
            DECODE(vaccine_category_group_id,13,2,vaccine_category_group_id) AS vaccine_category_group_id,
            evaluation_code
        FROM
            evaluation
        WHERE
            vaccine_category_group_id NOT IN ( /* exclude other and H1N1 vaccine groups */
                8,
                16
            )
            AND child_id =:child_id""", {"child_id":child_id})

    #
    # for each evaluation, check to see if it matches ICE, otherwise output the difference
    #
    for cvx,date_of_admin,vaccine_category_group_id,evaluation_code in ORA_CUR:
        key=date_of_admin+"|"+cvx+"|"+str(vaccine_category_group_id)
        if key in evaluations:
            if evaluation_code == 0 and evaluations[key] == "VALID^":
                pass
            elif evaluation_code == 3 and evaluations[key].startswith("INVALID^BELOW"):
                pass
            elif evaluation_code == 1 and evaluations[key].startswith("INVALID^BELOW"):
                pass
            elif evaluation_code == 9 and evaluations[key] == "ACCEPTED^EXTRA_DOSE":
                pass
            else:
                print("evaluation difference: %s _%s_ _%s_ : %s = %s" % (VGBC[str(vaccine_category_group_id)],key,child_id,EVAL[evaluation_code],evaluations[key]))
            
        else:
            print("evaluation missing: %s _%s_: %s " % (VGBC[str(vaccine_category_group_id)],child_id,key))

    #
    # second, compare the ICE recommendations (forecasts) to recommendations stored in the registry
    #

    recommendations = {}
    due_dates = {}
    overdue_dates = {}
    for recommendation in recommendation_list:
        if recommendation[pyiceclient.ICE_FORECASTS_GROUP] != 'Zoster Vaccine Group':
            key=str(VG[recommendation[pyiceclient.ICE_FORECASTS_GROUP]])
            recommendations[key] = recommendation[pyiceclient.ICE_FORECASTS_CONCEPT] + "^" + recommendation[pyiceclient.ICE_FORECASTS_INTERP] + "^" + recommendation[pyiceclient.ICE_FORECASTS_DUE_DATE] + "^" + recommendation[pyiceclient.ICE_FORECASTS_PAST_DUE_DATE]
            due_dates[key] = recommendation[pyiceclient.ICE_FORECASTS_DUE_DATE]
            overdue_dates[key] = recommendation[pyiceclient.ICE_FORECASTS_PAST_DUE_DATE]

    ORA_CUR.execute("""
        SELECT
             vaccine_category_group_id,
             recommendation_code,
             TO_CHAR(ns_due_date,'YYYYMMDD') AS ns_due_date,
             TO_CHAR(overdue_date,'YYYYMMDD') AS overdue_date
         FROM
             recommendation
         WHERE
             vaccine_category_group_id NOT IN ( /* exclude other and H1N1 vaccine groups */
                 8,
                 16
             )
             AND child_id =:child_id""", {"child_id":child_id})

    #
    # for each recommendation, check to see if it matches ICE (or is a known difference), otherwise output the difference
    #
    for vaccine_category_group_id,recommendation_code,ns_due_date,overdue_date in ORA_CUR:
        key=str(vaccine_category_group_id)
        if key in recommendations:
            if recommendation_code == 1 and ( recommendations[key] == "NOT_RECOMMENDED^COMPLETE^^" or recommendations[key] == "NOT_RECOMMENDED^COMPLETE_HIGH_RISK^^"):
                pass
            elif recommendation_code == 0 and recommendations[key].startswith("RECOMMENDED^DUE_NOW^" + ns_due_date):
                if overdue_dates[key] == overdue_date or VGBC[key] == "Influenza Vaccine Group":
                    pass
                else:
                    print ("recommendation overdue date mismatch: %s _%s_ : %s %s %s = %s "% (VGBC[key],child_id,recommendation_code,ns_due_date,overdue_date,recommendations[key]))
                pass
            elif recommendation_code == 0 and recommendations[key].startswith("RECOMMENDED^DUE_NOW^") and ns_due_date < datetime.date.today().strftime('%Y%m%d') and due_dates[key] < datetime.date.today().strftime('%Y%m%d'):
                if overdue_dates[key] == overdue_date or VGBC[key] == "Influenza Vaccine Group":
                    pass
                else:
                    print ("recommendation overdue date mismatch: %s _%s_ : %s %s %s = %s "% (VGBC[key],child_id,recommendation_code,ns_due_date,overdue_date,recommendations[key]))
                pass
            elif recommendation_code == 0 and recommendations[key].startswith("FUTURE_RECOMMENDED^DUE_IN_FUTURE^" + ns_due_date):
                pass
            elif recommendation_code == 2 and recommendations[key] == "NOT_RECOMMENDED^TOO_OLD^^":
                pass
            elif VGBC[key] == "Pneumococcal Vaccine Group" and recommendation_code == 1 and recommendations[key] == "CONDITIONAL^HIGH_RISK^^":
                pass
            elif VGBC[key] == "Influenza Vaccine Group" and recommendation_code == 0 and recommendations[key].startswith("RECOMMENDED^DUE_NOW^"):
                pass
            else:
                print ("recommendation mismatch: %s _%s_ : %s %s %s = %s " % (VGBC[key],child_id,recommendation_code,ns_due_date,overdue_date,recommendations[key]))
        else:
            print ("recommendation missing: _%s_ %s " % (child_id,key))
            print(recommendations)
