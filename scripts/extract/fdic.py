import pandas as pd
from pandas.tseries.offsets import QuarterEnd
import requests
from requests.adapters import HTTPAdapter
import time
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "utilities"))
import util

SOURCE_NAME = 'fdic'

### START OF EXTRACT METHODS ###

def extract_company(): 
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=10))

    institutions = pd.DataFrame()
    total = s.get("https://banks.data.fdic.gov/api/institutions").json()['totals']['count']
    limit = 10000
    offset = 0
    while len(institutions) < total:
        r = s.get("https://banks.data.fdic.gov/api/institutions?sort_by=ID&offset={}&limit={}".format(offset, limit))
        institutions = pd.concat([institutions, pd.json_normalize(r.json()['data'])])
        offset += limit
        time.sleep(5)
        
    institutions.columns = [x.replace('data.', '') for x in institutions.columns]
    
    print("Successfully extracted from API: institutions")

    return institutions


def extract_location():
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=10))

    locations = pd.DataFrame()
    total = s.get("https://banks.data.fdic.gov/api/locations").json()['totals']['count']
    limit = 10000
    offset = 0
    while len(locations) < total:
        r = s.get("https://banks.data.fdic.gov/api/locations?sort_by=ID&offset={}&limit={}".format(offset, limit))
        locations = pd.concat([locations, pd.json_normalize(r.json()['data'])])
        offset += limit
        time.sleep(5)

    locations.columns = [x.replace('data.', '') for x in locations.columns]
    
    print("Successfully extracted from API: locations")

    return locations


def extract_financials(): 
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=10))

    # get data starting Q12017 and ending two quarters ago today
    report_dates = []
    curr_date = pd.Timestamp(2017, 1, 1)
    while curr_date < pd.Timestamp.today().date() - QuarterEnd(2):
        curr_date += QuarterEnd()
        report_dates.append(str(curr_date.date()).replace('-', ''))

    # fields to pull
    demographics = 'CERT,REPDTE'
    #'ACTIVE,BKCLASS,CALLFORM,CB,CBSA,CERT,CITY,EFFDATE,ESTYMD,FDICDBS,FDICDBSDESC,FDICSUPV,FDICSUPVDESC,FED,FEDDESC,FLDOFF,INSAGNT1,INSDATE,MUTUAL,NAME,NAMEFULL,NAMEHCR,OCCDIST,OCCDISTDESC,OFFDOM,OFFFOR,OFFOA,OTSREGNM,PARCERT,QBPRCOMLDESC,REGAGNT,REPDTE,RSSDHCR,RSSDID,SPECGRP,SPECGRPDESC,STALP,STCNTY,SUBCHAPS,TRUST,ZIP'
    assets = 'CERT,ASSET,CHBAL,CHBALNI,CHBALI,CHCIC,CHITEM,CHCOIN,CHUS,CHNUS,CHFRB,SC,SCUS,SCUST,SCUSO,SCASPNSUM,SCFMN,SCGNM,SCCOL,SCCPTG,SCCMOG,SCMUNI,SCDOMO,SCRMBPI,SCCMOS,SCABS,SCSFP,SCODOT,SCFORD,SCEQNFT,SCEQ,SCEQFV,SCHTMRES,SCTATFR,SCPLEDGE,SCMTGBK,SCGTY,SCODPC,SCODPI,SCCMPT,SCCMOT,SCHA,SCAF,SCRDEBT,SCPT3LES,SCPT3T12,SCPT3T5,SCPT5T15,SCPTOV15,SCO3YLES,SCOOV3Y,SCNM3LES,SCNM3T12,SCNM1T3,SCNM3T5,SCNM5T15,SCNMOV15,SC1LES,SCSNHAA,SCSNHAF,TRADE,TRREVALSUM,TRLREVAL,FREPO,LNLSNET,LNATRES,LNLSGR,LNCONTRA,LNLSGRS,LNRE,LNREDOM,LNRECONS,LNRENRES,LNREMULT,LNRERES,LNREAG,LNREFOR,LNAG,LNCI,LNCON,LNCRCD,LNCONRP,LNAUTO,LNCONOTH,LNOTCI,LNFG,LNMUNI,LNSOTHER,LS,LNCOMRE,LNRENUS,LNPLEDGE,RB2LNRES,LNLSSALE,LNEXAMT,LNRENROW,LNRENROT,LNRERSFM,LNRERSF2,LNRELOC,LNRERSF1,LNRECNFM,LNRECNOT,RSLNLTOT,RSLNREFM,RSLNLS,P3RSLNLT,P3RSLNFM,P3RSLNLS,P9RSLNLT,P9RSLNFM,P9RSLNLS,NARSLNLT,NARSLNFM,NARSLNLS,LNLSGRF,UNINCFOR,LNLSFOR,LNDEPAOBK,LNDEPCBF,LNDEPUSF,LNDEPFCF,LNAGFOR,LNCIFOR,LNCINUSF,LNCONFOR,LNFGFOR,LNMUNIF,LNOTHERF,LSFOR,LNRS3LES,LNRS3T12,LNRS1T3,LNRS3T5,LNRS5T15,LNRSOV15,LNOT3LES,LNOT3T12,LNOT1T3,LNOT3T5,LNOT5T15,LNOTOV15,LNRENR4,LNRENR1,LNRENR2,LNRENR3,LNCI4,LNCI1,LNCI2,LNCI3,LNREAG4,LNREAG1,LNREAG2,LNREAG3,LNAG4,LNAG1,LNAG2,LNAG3,LNRENR4N,LNRENR1N,LNRENR2N,LNRENR3N,LNCI4N,LNCI1N,LNCI2N,LNCI3N,LNREAG4N,LNREAG1N,LNREAG2N,LNREAG3N,LNAG4N,LNAG1N,LNAG2N,LNAG3N,PPPLNNUM,PPPLNBAL,PPPLNPLG,PPPLF1LS,PPPLFOV1,AVPPPPLG,MMLFBAL,AVMMLF,BKPREM,ORE,OREINV,OREOTH,ORERES,OREMULT,ORENRES,ORECONS,OREAG,OREOTHF,INTAN,INTANGW,INTANMSR,INTANOTH,AOA'
    liabilities_equity = 'CERT,LIABEQ,LIAB,DEP,DEPDOM,ESTINS,TRN,TRNIPCOC,TRNUSGOV,TRNMUNI,TRNCBO,TRNFCFG,NTR,NTRIPC,NTRUSGOV,NTRMUNI,NTRCOMOT,NTRFCFG,DEPFOR,DEPIPCCF,DEPUSBKF,DEPFBKF,DEPFGOVF,DEPUSMF,DEPNIFOR,DEPIFOR,DDT,NTRSMMDA,NTRSOTH,TS,DEPNIDOM,DEPIDOM,COREDEP,DEPINS,DEPUNA,IRAKEOGH,BRO,BROINS,DEPLSNB,DEPCSBQ,DEPSMAMT,DEPSMB,DEPLGAMT,DEPLGB,DEPSMRA,DEPSMRN,DEPLGRA,DEPLGRN,TRNNIA,TRNNIN,NTRCDSM,NTRTMMED,NTRTMLGJ,CD3LESS,CD3T12S,CD1T3S,CDOV3S,CD3LES,CD3T12,CD1T3,CDOV3,FREPP,TRADEL,OTHBRF,OTBFH1L,OTBFH1T3,OTBFH3T5,OTBFHOV5,OTBFHSTA,OTBOT1L,OTBOT1T3,OTBOT3T5,OTBOTOV5,OTHBOT1L,SUBND,ALLOTHL,EQTOT,EQ,EQPP,EQCS,EQSUR,EQUPTOT,EQCONSUB,EQCPREV,EQCREST,NETINC,EQCSTKRX,EQCTRSTX,EQCMRG,EQCDIVP,EQCDIVC,EQCCOMPI,EQCBHCTR,ASSTLT,ASSET2,ASSET5,ERNAST,OALIFINS,OALIFGEN,OALIFHYB,OALIFSEP,AVASSETJ,RWAJT,RBCT2,RBCT1J,OTHBFHLB,VOLIAB'
    ratios = 'CERT,NTINCL,NTINCHPP,INTINCY,INTEXPY,NIMY,NONIIAY,NONIXAY,ELNATRY,NOIJY,ROA,ROAPTX,ROE,ROEINJR,NTLNLSR,NTRER,NTRECOSR,NTRENRSR,NTREMULR,NTRERESR,NTRELOCR,NTREOTHR,IDNTCIR,IDNTCONR,IDNTCRDR,IDNTCOOR,NTAUTOPR,NTCONOTR,NTALLOTHR,NTCOMRER,ELNANTR,IDERNCVR,EEFFR,ASTEMPM,EQCDIVNTINC,ERNASTR,LNATRESR,LNRESNCR,NPERFV,NCLNLS,NCLNLSR,NCRER,NCRECONR,NCRENRER,NCREMULR,NCRERESR,NCRELOCR,NCREREOR,IDNCCIR,IDNCCONR,IDNCCRDR,IDNCCOOR,IDNCATOR,IDNCCOTR,IDNCOTHR,NCCOMRER,IDNCGTPR,LNLSNTV,LNLSDEPR,IDLNCORR,DEPDASTR,EQV,RBC1AAJ,CBLRIND,IDT1CER,IDT1RWAJR,RBCRWAJ'

    financials = pd.DataFrame()
    limit = 10000
    for rd in report_dates:
        print(rd)
        curr_qtr_full = pd.DataFrame()
        for ftype in [demographics, assets, liabilities_equity, ratios]:
            curr_qtr_part = pd.DataFrame()
            total = s.get(
                "https://banks.data.fdic.gov/api/financials?filters=REPDTE:{}&fields={}&limit={}".format(
                    rd, ftype, limit)).json()['totals']['count']
            offset = 0
            while len(curr_qtr_part) < total:
                r = s.get(
                    "https://banks.data.fdic.gov/api/financials?filters=REPDTE:{}&fields={}&sort_by=ID&offset={}&limit={}".format(
                        rd, ftype, offset, limit))
                curr_qtr_part = pd.concat([curr_qtr_part, pd.json_normalize(r.json()['data'])])
                offset += limit
                time.sleep(5)
            assert curr_qtr_part['data.CERT'].value_counts().max() == 1
            if len(curr_qtr_full) == 0:
                curr_qtr_full = curr_qtr_part.copy()
            else:
                curr_qtr_full = curr_qtr_full.merge(curr_qtr_part.drop(
                    columns=[x for x in curr_qtr_part.columns if x in curr_qtr_full.columns and x != 'data.CERT']),
                    on='data.CERT', how='left')
        financials = pd.concat([financials, curr_qtr_full])

    financials.columns = [x.replace('data.', '') for x in financials.columns]
    
    print("Successfully extracted from API: financials")

    return financials


def load_raw_company():
    institutions = extract_company()
    dest_table_name = 'company'
    csv_file_name = SOURCE_NAME + '_' + dest_table_name + '.csv'
    util.load_raw_data(institutions, csv_file_name)

def load_raw_location():
    locations = extract_location()
    dest_table_name = 'location'
    csv_file_name = SOURCE_NAME + '_' + dest_table_name + '.csv'
    util.load_raw_data(locations, csv_file_name)
    
def load_raw_financials():
    financials = extract_financials()
    dest_table_name = 'financials'
    csv_file_name = SOURCE_NAME + '_' + dest_table_name + '.csv'
    util.load_raw_data(financials, csv_file_name)

### END OF EXTRACT METHODS ###


def extract(table_name='all'): 
    if table_name == 'all':
        load_raw_company()
        load_raw_location()
        load_raw_financials()
    elif table_name == 'company':
        load_raw_company()
    elif table_name == 'location':
        load_raw_location()
    elif table_name == 'financials':
        load_raw_financials()
    else:
        print('Invalid table name')
