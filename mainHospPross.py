

import pandas as pd
from tqdm import tqdm
import time

from pandarallel import pandarallel

import math


def getHospCode():
    outlist = []
    outlist.append(50802)
    outlist.append(50804)
    outlist.append(50805)
    outlist.append(50813)
    outlist.append(50817)
    outlist.append(50818)
    outlist.append(50820)
    outlist.append(50821)
    outlist.append(50852)
    outlist.append(50854)
    outlist.append(50855)
    outlist.append(50861)
    outlist.append(50862)
    outlist.append(50863)
    outlist.append(50867)
    outlist.append(50868)
    outlist.append(50878)
    outlist.append(50882)
    outlist.append(50883)
    outlist.append(50884)
    outlist.append(50885)
    outlist.append(50889)
    outlist.append(50890)
    outlist.append(50891)
    outlist.append(50893)
    outlist.append(50902)
    outlist.append(50904)
    outlist.append(50905)
    outlist.append(50907)
    outlist.append(50910)
    outlist.append(50912)
    outlist.append(50920)
    outlist.append(50922)
    outlist.append(50927)
    outlist.append(50931)
    outlist.append(50949)
    outlist.append(50950)
    outlist.append(50951)
    outlist.append(50956)
    outlist.append(50960)
    outlist.append(50971)
    outlist.append(50983)
    outlist.append(51000)
    outlist.append(51006)
    outlist.append(51007)
    outlist.append(51080)
    outlist.append(51081)
    outlist.append(51082)
    outlist.append(51105)
    outlist.append(51108)
    outlist.append(51109)
    outlist.append(51133)
    outlist.append(51146)
    outlist.append(51199)
    outlist.append(51200)
    outlist.append(51221)
    outlist.append(51222)
    outlist.append(51230)
    outlist.append(51237)
    outlist.append(51244)
    outlist.append(51245)
    outlist.append(51248)
    outlist.append(51249)
    outlist.append(51250)
    outlist.append(51253)
    outlist.append(51254)
    outlist.append(51256)
    outlist.append(51265)
    outlist.append(51274)
    outlist.append(51275)
    outlist.append(51279)
    outlist.append(51300)
    outlist.append(51301)
    outlist.append(51492)
    outlist.append(51498)
    outlist.append(51536)
    outlist.append(51537)
    outlist.append(51538)
    outlist.append(51574)
    outlist.append(51575)
    outlist.append(51576)
    outlist.append(51577)
    outlist.append(51578)
    outlist.append(51594)
    outlist.append(51595)
    outlist.append(51596)
    outlist.append(51623)
    outlist.append(51624)
    outlist.append(51640)
    outlist.append(51652)
    outlist.append(51674)
    outlist.append(51675)
    outlist.append(51690)
    outlist.append(51697)
    outlist.append(51701)
    outlist.append(51704)
    outlist.append(51755)
    outlist.append(51756)
    outlist.append(51977)
    outlist.append(51992)
    outlist.append(51994)
    outlist.append(52000)
    outlist.append(52022)
    outlist.append(52026)
    outlist.append(52029)
    outlist.append(52073)
    outlist.append(52074)
    outlist.append(52075)
    outlist.append(52101)
    outlist.append(52116)
    outlist.append(52144)
    outlist.append(52163)
    outlist.append(52164)
    outlist.append(52165)
    outlist.append(52166)
    outlist.append(52167)
    outlist.append(52170)
    outlist.append(52442)
    outlist.append(52500)
    outlist.append(52535)
    outlist.append(52546)
    outlist.append(52610)
    outlist.append(52623)
    outlist.append(52647)
    outlist.append(52769)
    outlist.append(52921)
    outlist.append(52923)
    return outlist


def loadHospData():
    print('read_csv')
    loadHospDf = pd.read_csv('2021_11_21_23_10_47_mimic_hosp.csv')
    #loadHospDf = loadHospDf[:50000]
    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    print('select code')
    loadHospDf = loadHospDf.sort_values(by=['itemid'])
    loadHospDf = loadHospDf[loadHospDf['itemid'].isin(getHospCode())]
    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    print('sort_values')
    loadHospDf = loadHospDf.sort_values(
        by=['subject_id', 'charttime', 'hadm_id', 'itemid'])
    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    print('split by subject_id')
    allSubject_id = pd.unique(loadHospDf['subject_id'])

    dtLen = len(allSubject_id)
    base = 20
    devi = math.ceil(dtLen/base)

    outDflist = []
    for ii in tqdm(range(base)):
        kpp = allSubject_id[(ii*devi):((ii+1)*devi)-1]
        if len(kpp) > 0:
            sppDf = loadHospDf[loadHospDf['subject_id'].isin(kpp)]
            if len(sppDf) > 0:
                outDflist.append(sppDf)

                try:
                    print('save split hosp data')
                    saveTimeStr = time.strftime(
                        "%Y_%m_%d_%H_%M_%S", time.localtime())
                    sppDf.to_csv(saveTimeStr+'mimic_iv_hosp_part' +
                                 str(ii)+'.csv', index=False)
                    saveTimeStr = time.strftime(
                        "%Y_%m_%d_%H_%M_%S", time.localtime())
                    print(saveTimeStr)
                except:
                    print('error : loadHospData save to csv')

    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    return outDflist


def hsFunc01(inputDf):
    outDf = pd.DataFrame()
    outDf.loc[0, ['subject_id', 'charttime', 'hadm_id']
              ] = inputDf.iloc[0][['subject_id', 'charttime', 'hadm_id']]

    for ii in range(len(inputDf)):
        try:
            outDf.loc[0, 'hosp'+str(inputDf.iloc[ii]['itemid'])
                      ] = float(inputDf.iloc[ii]['valuenum'])
        except:
            outDf.loc[0, 'hosp'+str(inputDf.iloc[ii]['itemid'])
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
                    outDf.loc[0, 'hosp'+str(allItem[ii])
                              ] = float(tyy.iloc[(len(tyy)-1)]['valuenum'])
                except:
                    print('error : hsFunc01_02 valuenum')

            else:
                try:
                    tyy = tyy.sort_values(by=['charttime'])
                    outDf.loc[0, 'hosp'+str(allItem[ii])
                              ] = float(tyy.iloc[0]['valuenum'])
                except:
                    print('error : hsFunc01_02 charttime')

        except:
            tyy = tyy.sort_values(by=['charttime'])
            outDf.loc[0, 'hosp'+str(allItem[ii])] = tyy.iloc[0]['value']

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

        return iGroupby

    except:
        print('error : rtGroupby indatafram.groupby')
        print(len(indatafram))
        iGroupby = (pd.DataFrame, 0)

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
        inpuDf.to_csv(saveTimeStr+'mimic_iv_hosp_group_part' +
                      str(atPart)+'.csv', index=False)
    except:
        print('error save csv')

    return 0


def setbyset():
    saveTimeStr = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())
    print(saveTimeStr)

    splitHosp = loadHospData()

    for ii in tqdm(range(len(splitHosp))):

        if len(splitHosp[ii]) > 0:
            tmpGroup = rtGroupby(splitHosp[ii])
            if len(tmpGroup) > 0:
                tmpDf = reconcat(tmpGroup)
                ttt = toSave(tmpDf, ii)

    print('done')
    return 0


if __name__ == "__main__":
    # tqdm.pandas()
    pandarallel.initialize(progress_bar=True, nb_workers=20)
    ret = setbyset()
