

import pandas as pd
from tqdm import tqdm
import time

from pandarallel import pandarallel

import math


def getIcuCode():
    outlist = []
    outlist.append(220003)
    outlist.append(220048)
    outlist.append(220224)
    outlist.append(220227)
    outlist.append(220228)
    outlist.append(220235)
    outlist.append(220545)
    outlist.append(220546)
    outlist.append(220580)
    outlist.append(220581)
    outlist.append(220587)
    outlist.append(220602)
    outlist.append(220603)
    outlist.append(220615)
    outlist.append(220621)
    outlist.append(220624)
    outlist.append(220635)
    outlist.append(220644)
    outlist.append(220645)
    outlist.append(220650)
    outlist.append(223830)
    outlist.append(224639)
    outlist.append(224828)
    outlist.append(225624)
    outlist.append(225625)
    outlist.append(225628)
    outlist.append(225634)
    outlist.append(225637)
    outlist.append(225638)
    outlist.append(225639)
    outlist.append(225640)
    outlist.append(225641)
    outlist.append(225642)
    outlist.append(225643)
    outlist.append(225651)
    outlist.append(225667)
    outlist.append(225671)
    outlist.append(225672)
    outlist.append(225674)
    outlist.append(225677)
    outlist.append(225690)
    outlist.append(225692)
    outlist.append(225693)
    outlist.append(226512)
    outlist.append(226512)
    outlist.append(226730)
    outlist.append(226738)
    outlist.append(226739)
    outlist.append(226979)
    outlist.append(226980)
    outlist.append(227073)
    outlist.append(227088)
    outlist.append(227443)
    outlist.append(227444)
    outlist.append(227444)
    outlist.append(227445)
    outlist.append(227456)
    outlist.append(227465)
    outlist.append(227466)
    outlist.append(227468)
    outlist.append(227470)
    outlist.append(227471)
    outlist.append(227516)
    outlist.append(227543)
    outlist.append(228685)
    outlist.append(229356)

    return outlist


def loadIcuData():
    # ,subject_id,hadm_id,stay_id,charttime,storetime,itemid,value,valuenum,valueuom,warning
    print('read_csv')
    loadIcuDf = pd.read_csv('2021_11_16_01_41_05_mimic_icu.csv')
    #loadIcuDf = loadIcuDf[:500000]
    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    print('select code')
    loadIcuDf = loadIcuDf.sort_values(by=['itemid'])
    loadIcuDf = loadIcuDf[loadIcuDf['itemid'].isin(getIcuCode())]
    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    print('sort_values')
    loadIcuDf = loadIcuDf.sort_values(
        by=['subject_id', 'charttime', 'hadm_id', 'stay_id', 'itemid'])
    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    print('split by subject_id')
    allSubject_id = pd.unique(loadIcuDf['subject_id'])

    dtLen = len(allSubject_id)
    base = 20
    devi = math.ceil(dtLen/base)

    outDflist = []
    for ii in tqdm(range(base)):
        kpp = allSubject_id[(ii*devi):((ii+1)*devi)-1]
        if len(kpp) > 0:
            sppDf = loadIcuDf[loadIcuDf['subject_id'].isin(kpp)]
            if len(sppDf) > 0:
                outDflist.append(sppDf)

                try:
                    print('save split Icu data')
                    saveTimeStr = time.strftime(
                        "%Y_%m_%d_%H_%M_%S", time.localtime())
                    sppDf.to_csv(saveTimeStr+'mimic_iv_Icu_part' +
                                 str(ii)+'.csv', index=False)
                    saveTimeStr = time.strftime(
                        "%Y_%m_%d_%H_%M_%S", time.localtime())
                    print(saveTimeStr)
                except:
                    print('error : loadIcuData save to csv')

    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    return outDflist


def hsFunc01(inputDf):
    outDf = pd.DataFrame()
    outDf.loc[0, ['subject_id', 'charttime', 'hadm_id']
              ] = inputDf.iloc[0][['subject_id', 'charttime', 'hadm_id']]

    for ii in range(len(inputDf)):
        try:
            outDf.loc[0, 'Icu'+str(inputDf.iloc[ii]['itemid'])
                      ] = float(inputDf.iloc[ii]['valuenum'])
        except:
            outDf.loc[0, 'Icu'+str(inputDf.iloc[ii]['itemid'])
                      ] = inputDf.iloc[ii]['value']

    return outDf, len(inputDf)


def hsFunc01_02(inputDf):
    outDf = pd.DataFrame()
    outDf.loc[0, ['subject_id', 'hadm_id']
              ] = inputDf.iloc[0][['subject_id', 'hadm_id']]

    chtime = inputDf.sort_values(by=['charttime'])
    outDf.loc[0, 'charttime'] = chtime.iloc[0]['charttime']
    outDf.loc[0, 'charttime_Last'] = chtime.iloc[(len(inputDf)-1)]['charttime']

    allItem = pd.unique(inputDf['itemid'])

    for ii in range(len(allItem)):
        tyy = inputDf[inputDf['itemid'] == allItem[ii]]

        try:
            aaa = float(tyy.iloc[0]['valuenum'])

            if len(tyy) >= 2:
                try:
                    tyy = tyy.sort_values(by=['valuenum'])
                    outDf.loc[0, 'Icu'+str(allItem[ii])
                              ] = float(tyy.iloc[(len(tyy)-1)]['valuenum'])
                except:
                    print('error : hsFunc01_02 valuenum')

            else:
                try:
                    tyy = tyy.sort_values(by=['charttime'])
                    outDf.loc[0, 'Icu'+str(allItem[ii])
                              ] = float(tyy.iloc[0]['valuenum'])
                except:
                    print('error : hsFunc01_02 charttime')

        except:
            tyy = tyy.sort_values(by=['charttime'])
            outDf.loc[0, 'Icu'+str(allItem[ii])] = tyy.iloc[0]['value']

    return outDf, len(inputDf)


def rtGroupby(indatafram):

    print('go groupby')
    try:
        goParallel = 1

        '''if goParallel == 0:
            iGroupby = indatafram.groupby(
                by=['subject_id','charttime', 'hadm_id']).apply(hsFunc01)
        elif goParallel == 1:
            iGroupby = indatafram.groupby(
                by=['subject_id','charttime', 'hadm_id']).parallel_apply(hsFunc01)'''
        if goParallel == 0:
            iGroupby = indatafram.groupby(
                by=['subject_id', 'hadm_id']).apply(hsFunc01_02)
        elif goParallel == 1:
            iGroupby = indatafram.groupby(
                by=['subject_id', 'hadm_id']).parallel_apply(hsFunc01_02)

        saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
        print(saveTimeStr)
        return iGroupby

    except:
        print('error : rtGroupby indatafram.groupby')
        print(len(indatafram))

    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    return []


def scFunc02(inputDf):

    try:
        if 0 < len(inputDf) and len(inputDf) <= 2:
            outDf = pd.DataFrame()
            for ii in (range(len(inputDf))):
                if ii == 0:
                    outDf = inputDf.iloc[ii][0]
                else:
                    tmp = inputDf.iloc[ii][0]
                    outDf = pd.concat(
                        [outDf, tmp], ignore_index=True, sort=False)
            return outDf
        elif 2 < len(inputDf):
            cutt = int(math.ceil(len(inputDf)*0.5))
            partA = inputDf.iloc[0:cutt]
            partB = inputDf.iloc[(len(inputDf)-cutt+1):]

            if len(partA) > 0 and len(partB) > 0:
                return pd.concat([scFunc02(partA), scFunc02(partB)], ignore_index=True, sort=False)
            elif len(partA) > 0 and len(partB) == 0:
                return partA
            elif len(partA) == 0 and len(partB) > 0:
                return partB
            elif len(partA) == 0 and len(partB) == 0:
                return pd.DataFrame()

    except:
        print('error scFunc02')

    return pd.DataFrame()


def reconcat(inputGroup):
    print('go concat power by scFunc02()')
    hpartDf = scFunc02(inputGroup)
    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)
    return hpartDf


def toSave(inpuDf, atPart):
    print('sort_values')
    inpuDf = inpuDf.sort_values(by=['subject_id', 'charttime', 'hadm_id'])
    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    print('save to csv')
    try:
        inpuDf.to_csv(saveTimeStr+'mimic_iv_Icu_group_part' +
                      str(atPart)+'.csv', index=False)
        saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
        print(saveTimeStr)
    except:
        print('error save csv')

    return 0


def setbyset():
    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    splitIcu = loadIcuData()

    for ii in tqdm(range(len(splitIcu))):

        if len(splitIcu[ii]) > 0:
            tmpGroup = rtGroupby(splitIcu[ii])
            if len(tmpGroup) > 0:
                tmpDf = reconcat(tmpGroup)
                ttt = toSave(tmpDf, ii)

    print('done')
    return 0


if __name__ == "__main__":
    # tqdm.pandas()
    pandarallel.initialize(progress_bar=True, nb_workers=20)
    ret = setbyset()
