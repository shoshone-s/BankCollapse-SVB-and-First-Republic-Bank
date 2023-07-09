import pandas as pd
from pandas.tseries.offsets import QuarterEnd
import requests
from requests.adapters import HTTPAdapter
import time
import aws_read_write

# read credentials from the config file
cfg_data = configparser.ConfigParser()
cfg_data.read("keys_config.cfg")
S3_BUCKET_NAME = cfg_data["S3"]["bucket_name"]

### START OF EXTRACT METHODS ###

def extract_locations():
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
    demographics = 'ACTIVE,BKCLASS,CALLFORM,CB,CBSA,CERT,CITY,EFFDATE,ESTYMD,FDICDBS,FDICDBSDESC,FDICSUPV,FDICSUPVDESC,FED,FEDDESC,FLDOFF,INSAGNT1,INSDATE,MUTUAL,NAME,NAMEFULL,NAMEHCR,OCCDIST,OCCDISTDESC,OFFDOM,OFFFOR,OFFOA,OTSREGNM,PARCERT,QBPRCOMLDESC,REGAGNT,REPDTE,RSSDHCR,RSSDID,SPECGRP,SPECGRPDESC,STALP,STCNTY,SUBCHAPS,TRUST,ZIP'
    assets = 'CERT,RSSDHCR,NAMEFULL,CITY,STALP,ZIP,REPDTE,BKCLASS,NAMEHCR,OFFDOM,SPECGRP,SUBCHAPS,ESTYMD,INSDATE,EFFDATE,MUTUAL,PARCERT,TRUST,REGAGNT,INSAGNT1,FDICDBS,FDICSUPV,FLDOFF,FED,OCCDIST,OTSREGNM,OFFOA,CB,ASSET,CHBAL,CHBALNI,CHBALI,CHCIC,CHITEM,CHCOIN,CHUS,CHNUS,CHFRB,SC,SCUS,SCUST,SCUSO,SCASPNSUM,SCFMN,SCGNM,SCCOL,SCCPTG,SCCMOG,SCMUNI,SCDOMO,SCRMBPI,SCCMOS,SCABS,SCSFP,SCODOT,SCFORD,SCEQNFT,SCEQ,SCEQFV,SCHTMRES,SCTATFR,SCPLEDGE,SCMTGBK,SCGTY,SCODPC,SCODPI,SCCMPT,SCCMOT,SCHA,SCAF,SCRDEBT,SCPT3LES,SCPT3T12,SCPT3T5,SCPT5T15,SCPTOV15,SCO3YLES,SCOOV3Y,SCNM3LES,SCNM3T12,SCNM1T3,SCNM3T5,SCNM5T15,SCNMOV15,SC1LES,SCSNHAA,SCSNHAF,TRADE,TRREVALSUM,TRLREVAL,FREPO,LNLSNET,LNATRES,LNLSGR,LNCONTRA,LNLSGRS,LNRE,LNREDOM,LNRECONS,LNRENRES,LNREMULT,LNRERES,LNREAG,LNREFOR,LNAG,LNCI,LNCON,LNCRCD,LNCONRP,LNAUTO,LNCONOTH,LNOTCI,LNFG,LNMUNI,LNSOTHER,LS,LNCOMRE,LNRENUS,LNPLEDGE,RB2LNRES,LNLSSALE,LNEXAMT,LNRENROW,LNRENROT,LNRERSFM,LNRERSF2,LNRELOC,LNRERSF1,LNRECNFM,LNRECNOT,RSLNLTOT,RSLNREFM,RSLNLS,P3RSLNLT,P3RSLNFM,P3RSLNLS,P9RSLNLT,P9RSLNFM,P9RSLNLS,NARSLNLT,NARSLNFM,NARSLNLS,LNLSGRF,UNINCFOR,LNLSFOR,LNDEPAOBK,LNDEPCBF,LNDEPUSF,LNDEPFCF,LNAGFOR,LNCIFOR,LNCINUSF,LNCONFOR,LNFGFOR,LNMUNIF,LNOTHERF,LSFOR,LNRS3LES,LNRS3T12,LNRS1T3,LNRS3T5,LNRS5T15,LNRSOV15,LNOT3LES,LNOT3T12,LNOT1T3,LNOT3T5,LNOT5T15,LNOTOV15,LNRENR4,LNRENR1,LNRENR2,LNRENR3,LNCI4,LNCI1,LNCI2,LNCI3,LNREAG4,LNREAG1,LNREAG2,LNREAG3,LNAG4,LNAG1,LNAG2,LNAG3,LNRENR4N,LNRENR1N,LNRENR2N,LNRENR3N,LNCI4N,LNCI1N,LNCI2N,LNCI3N,LNREAG4N,LNREAG1N,LNREAG2N,LNREAG3N,LNAG4N,LNAG1N,LNAG2N,LNAG3N,PPPLNNUM,PPPLNBAL,PPPLNPLG,PPPLF1LS,PPPLFOV1,AVPPPPLG,MMLFBAL,AVMMLF,BKPREM,ORE,OREINV,OREOTH,ORERES,OREMULT,ORENRES,ORECONS,OREAG,OREOTHF,INTAN,INTANGW,INTANMSR,INTANOTH,AOA'
    liabilities_equity = 'CERT,LIABEQ,LIAB,DEP,DEPDOM,ESTINS,TRN,TRNIPCOC,TRNUSGOV,TRNMUNI,TRNCBO,TRNFCFG,NTR,NTRIPC,NTRUSGOV,NTRMUNI,NTRCOMOT,NTRFCFG,DEPFOR,DEPIPCCF,DEPUSBKF,DEPFBKF,DEPFGOVF,DEPUSMF,DEPNIFOR,DEPIFOR,DDT,NTRSMMDA,NTRSOTH,TS,DEPNIDOM,DEPIDOM,COREDEP,DEPINS,DEPUNA,IRAKEOGH,BRO,BROINS,DEPLSNB,DEPCSBQ,DEPSMAMT,DEPSMB,DEPLGAMT,DEPLGB,DEPSMRA,DEPSMRN,DEPLGRA,DEPLGRN,TRNNIA,TRNNIN,NTRCDSM,NTRTMMED,NTRTMLGJ,CD3LESS,CD3T12S,CD1T3S,CDOV3S,CD3LES,CD3T12,CD1T3,CDOV3,FREPP,TRADEL,OTHBRF,OTBFH1L,OTBFH1T3,OTBFH3T5,OTBFHOV5,OTBFHSTA,OTBOT1L,OTBOT1T3,OTBOT3T5,OTBOTOV5,OTHBOT1L,SUBND,ALLOTHL,EQTOT,EQ,EQPP,EQCS,EQSUR,EQUPTOT,EQCONSUB,EQCPREV,EQCREST,NETINC,EQCSTKRX,EQCTRSTX,EQCMRG,EQCDIVP,EQCDIVC,EQCCOMPI,EQCBHCTR,ASSTLT,ASSET2,ASSET5,ERNAST,OALIFINS,OALIFGEN,OALIFHYB,OALIFSEP,AVASSETJ,RWAJT,RBCT2,RBCT1J,OTHBFHLB,VOLIAB'
    ratios = 'CERT,RSSDHCR,NAMEFULL,CITY,STALP,ZIP,REPDTE,BKCLASS,NAMEHCR,OFFDOM,SPECGRP,SUBCHAPS,ESTYMD,INSDATE,EFFDATE,MUTUAL,PARCERT,TRUST,REGAGNT,INSAGNT1,FDICDBS,FDICSUPV,FLDOFF,FED,OCCDIST,OTSREGNM,OFFOA,CB,NTINCL,NTINCHPP,INTINCY,INTEXPY,NIMY,NONIIAY,NONIXAY,ELNATRY,NOIJY,ROA,ROAPTX,ROE,ROEINJR,NTLNLSR,NTRER,NTRECOSR,NTRENRSR,NTREMULR,NTRERESR,NTRELOCR,NTREOTHR,IDNTCIR,IDNTCONR,IDNTCRDR,IDNTCOOR,NTAUTOPR,NTCONOTR,NTALLOTHR,NTCOMRER,ELNANTR,IDERNCVR,EEFFR,ASTEMPM,EQCDIVNTINC,ERNASTR,LNATRESR,LNRESNCR,NPERFV,NCLNLS,NCLNLSR,NCRER,NCRECONR,NCRENRER,NCREMULR,NCRERESR,NCRELOCR,NCREREOR,IDNCCIR,IDNCCONR,IDNCCRDR,IDNCCOOR,IDNCATOR,IDNCCOTR,IDNCOTHR,NCCOMRER,IDNCGTPR,LNLSNTV,LNLSDEPR,IDLNCORR,DEPDASTR,EQV,RBC1AAJ,CBLRIND,IDT1CER,IDT1RWAJR,RBCRWAJ'

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

    return financials

def extract_institutions(): 
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

    return institutions


def load_raw_institutions():
    institutions = extract_institutions()
    institutions.columns = [x.replace('data.', '') for x in institutions.columns]
    institutions.to_csv('../../data/institutions.csv', index=False)


def load_raw_financials():
    financials_df = extract_financials()
    # financials_df.to_parquet('../../data/financials.parquet') # FIXME: I don't think we need the paquet if we have the csv
    financials_df.to_csv('../../data/financials.csv', index=False)

def load_raw_locations():
    locations = extract_locations()
    locations.columns = [x.replace('data.', '') for x in locations.columns]
    locations.to_csv('../../data/raw_data/locations.csv', index=False)


### END OF EXTRACT METHODS ###

### START OF TRANSFORM METHODS ###

def transform_locations(): 
    # selected banks from FDIC
    CERT_LIST = [24735, 59017, 21761, 628, 29147, 27389, 3511, 5146, 18409, 33947, 7213, 3510, 34968, 57803]

    locations_df = aws_read_write.get_csv(bucket_name=S3_BUCKET_NAME, object_name='raw_data/locations.csv')

    clean_locations = locations_df[locations_df.CERT.isin(CERT_LIST)].sort_values('NAME')[['CERT','NAME','MAINOFF','OFFNAME','ESTYMD','SERVTYPE','ADDRESS','COUNTY','CITY','STNAME','ZIP','LATITUDE','LONGITUDE']].rename(columns={'NAME':'company_name', 'MAINOFF':'main_office', 'OFFNAME':'branch_name', 'ESTYMD':'established_date', 'SERVTYPE':'service_type', 'STNAME':'state'})
    clean_locations.columns = [x.lower() for x in clean_locations.columns]
    clean_locations['established_date'] = pd.to_datetime(clean_locations['established_date'])
    clean_locations['service_type'] = clean_locations['service_type'].replace({11:'Full Service Brick and Mortar Office', 12:'Full Service Retail Office', 13:'Full Service Cyber Office', 14:'Full Service Mobile Office', 15:'Full Service Home/Phone Banking', 16:'Full Service Seasonal Office', 21:'Limited Service Administrative Office', 22:'Limited Service Military Facility', 23:'Limited Service Facility Office', 24:'Limited Service Loan Production Office', 25:'Limited Service Consumer Credit Office', 26:'Limited Service Contractual Office', 27:'Limited Service Messenger Office', 28:'Limited Service Retail Office', 29:'Limited Service Mobile Office', 30:'Limited Service Trust Office'})
    clean_locations['zip'] = clean_locations['zip'].astype(str)
    clean_locations.loc[~clean_locations.state.isin(['Puerto Rico','Virgin Islands Of The U.S.']) & clean_locations.zip.apply(lambda x: len(x)!=5), 'zip'] = clean_locations['zip'].str.zfill(5)

    clean_locations.to_csv(data_path + "\\clean_locations.csv", index=False)
    aws_read_write.upload_file(file_name=data_path + '\\clean_locations.csv', bucket_name=S3_BUCKET_NAME, object_name='clean_data/locations.csv')


### END OF TRANSFORM METHODS ###



