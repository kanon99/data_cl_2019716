import pandas as pd
import numpy as np
from clean_done.maina import *

#把多余的字符串删除
def clesrningg(Now_N_sheets,j):

    a = str(todaota(Now_N_sheets,j))
    list = todaota(Now_N_sheets,j)

    #统计标题字符串数
    strnumbers = a.find("-")
    strnumbers1 = a.find("*")
    strnumbers3 = a.find(" ")
    strnumbers13 = a.find("\n")

    #统计标题字符串数
    strnumbers4= a.find("*")
    strnumbers5= a.find("\'")
    strnumbers6= a.find("\n")
    strnumbers7= a.find("\r")
    strnumbers8= a.find("\f")
    strnumbers9= a.find("\'")
    strnumbers10= a.find("\;")
    strnumbers11= a.find(" ")

    if strnumbers > 0:
        aa = list.rename(columns=lambda x: x.replace('-', ''), inplace=True)
    if strnumbers1 > 0:
        aa = list.rename(columns=lambda x: x.replace('*', ''), inplace=True)
    if strnumbers3 > 0:
        aa = list.rename(columns=lambda x: x.replace(' ', ''), inplace=True)
    if strnumbers13 > 0:
        aa = list.rename(columns=lambda x: x.replace('\n', ''), inplace=True)
    if strnumbers4 > 0:
        aa = list.replace("*",'')
    if strnumbers5 > 0:
        aa = list.replace("\'",'')
    if strnumbers6 > 0:
        aa = list.replace("\n",'')
    if strnumbers7 > 0:
        aa = list.replace("\r",'')
    if strnumbers8 > 0:
        aa = list.replace("\f",'')
    if strnumbers9 > 0:
        aa = list.replace("\'",'')
    if strnumbers10 > 0:
        aa = list.replace("\;",'')
    if strnumbers11 > 0:
        aa = list.replace(" ",'')

    aa = list.replace('', 'missing')

    #a10=a9.to_csv('Result.csv', encoding="gbk")
    return aa


# def arraytolist(self):
#
#     aa = clesrningg(self)
#     bb = np.array(aa)


#模糊匹配特定字符串并模糊匹配出相关字段的列信息
class findallneedstr(object):

    def findstr(rlist2,onestr,Now_N_sheets,Nexcels):
        rlist2 = clesrningg(Now_N_sheets,Nexcels)
        found = []
        for element in rlist2:
            if onestr in element:
                found.append(element)
        return found

    def findstr2(onestr,Now_N_sheets,Nexcels):

        aa = clesrningg(Now_N_sheets,Nexcels)

        global new_list_jobs,new_list_stores,new_list_Hire,new_list_sex,new_list_cardnum,new_list_cardtype,new_list,new_list1,new_list2,new_list3,new_list_ryzt,\
            new_list_zhiwei,new_list_yfgz,new_list_shgz,new_list_shifa,new_list_jbgz,new_list_qqj,new_list_gdgzze,new_list_nlgz,new_list_gljt,new_list_cb,\
            new_list_qqkk,new_list_fdgzze,new_list_yzgz

        new_list_Hire = []
        new_list = []
        new_list3 = []
        new_list1 = []
        new_list2 = []
        new_list_cardtype = []
        new_list_cardnum = []
        new_list_sex = []
        new_list_stores = []
        new_list_jobs = []
        new_list_ryzt = []
        new_list_zhiwei = []
        new_list_yfgz = []
        new_list_shgz = []
        new_list_shifa = []
        new_list_jbgz = []
        new_list_qqj = []
        new_list_gdgzze = []
        new_list_nlgz = []
        new_list_gljt = []
        new_list_cb = []
        new_list_qqkk = []
        new_list_fdgzze = []
        new_list_yzgz = []


        # 打印出模糊匹配的字符：打印出任职的姓名列的单元格字段
        onestr = "姓名"
        name_ = findallneedstr().findstr(onestr,Now_N_sheets,Nexcels)
        if name_ != []:
            #把姓名列转为为array：
            name_array = np.array(aa[name_])
            #获取名字list
            #print(name_array)
            for i in range(len(name_array)):
                new_list.append(name_array[i,0])

            # 打印出模糊匹配的字符：打印出任职的入职日期列的单元格字段
            onestr = "入职"
            enter_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if enter_ == []:
                o = 0
                new_list3 = []
                for o in range(len(new_list)):
                    new_list3.append("missing")
                    o += 1


            else:
                if enter_ != []:
                    enter_array = np.array(aa[enter_])
                    # 获取入职list

                    for i in range(len(enter_array)):
                        new_list3.append(enter_array[i, 0])

                else:
                    pass

            # 打印出模糊匹配的字符：打印出任职的离职列的单元格字段
            onestr = "离职"
            fire_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if fire_ == []:
                o = 0
                new_list1 = []
                for o in range(len(new_list)):
                    new_list1.append("missing")
                    o += 1
                # print('离职',new_list1)

            else:
                if fire_ != []:
                    findallneedstr_array = np.array(aa[fire_])
                    # 获取离职list

                    for i in range(len(findallneedstr_array)):
                        new_list1.append(findallneedstr_array[i, 0])

                else:
                    pass

            # 打印出模糊匹配的字符：打印出任职的手机列的单元格字段
            onestr = "手机"
            phone_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            # print(phone_)
            if phone_ == []:
                o = 0
                new_list2 = []
                for o in range(len(new_list)):
                    new_list2.append("missing")
                    o += 1
                # print('手机',new_list2)

            else:
                if phone_ != []:
                    phone_array = np.array(aa[phone_])
                    # 获取手机list

                    for i in range(len(phone_array)):
                        new_list2.append(phone_array[i, 0])

                else:
                    pass

            # 打印出模糊匹配的字符：
            onestr = "证照类型"
            cardtype_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            # print(phone_)
            if cardtype_ == []:
                o = 0
                new_list_cardtype = []
                for o in range(len(new_list)):
                    new_list_cardtype.append("missing")
                    o += 1
                # print('证照类型', new_list_cardtype)

            else:
                if cardtype_ != []:
                    cardtype_array = np.array(aa[cardtype_])
                    # 获取手机list

                    for i in range(len(cardtype_array)):
                        new_list_cardtype.append(cardtype_array[i, 0])

                else:
                    pass

                # 打印出模糊匹配的字符：
            onestr = "证照号码"
            cardnum_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            # print(phone_)
            if cardnum_ == []:
                o = 0
                new_list_cardnum = []
                for o in range(len(new_list)):
                    new_list_cardnum.append("missing")
                    o += 1
                # print('证照号码', new_list_cardnum)

            else:
                if cardnum_ != []:
                    cardnum_array = np.array(aa[cardnum_])
                    # 获取手机list

                    for i in range(len(cardnum_array)):
                        new_list_cardnum.append(cardnum_array[i, 0])

                else:
                    pass

                # 打印出模糊匹配的字符：
            onestr = "性别"
            sex_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            # print(phone_)
            if sex_ == []:
                o = 0
                new_list_sex = []
                for o in range(len(new_list)):
                    new_list_sex.append("missing")
                    o += 1
                # print('性别', new_list_sex)

            else:
                if sex_ != []:
                    sex_array = np.array(aa[sex_])
                    # 获取手机list

                    for i in range(len(sex_array)):
                        new_list_sex.append(sex_array[i, 0])

                else:
                    pass

                # 打印出模糊匹配的字符：
            onestr = "受雇"
            Hier_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            # print(phone_)
            if Hier_ == []:
                o = 0
                new_list_Hire = []
                for o in range(len(new_list)):
                    new_list_Hire.append("missing")
                    o += 1
                # print('任职受雇', new_list_Hire)

            else:
                if Hier_ != []:
                    Hier_array = np.array(aa[Hier_])
                    # 获取手机list

                    for i in range(len(Hier_array)):
                        new_list_Hire.append(Hier_array[i, 0])
                    # print('任职受雇', new_list_Hire)
                    # print(len(new_list_Hire))
                else:
                    pass

                # 打印出模糊匹配的字符：
            onestr = "门店"
            stores_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            # print(phone_)
            if stores_ == []:
                o = 0
                new_list_stores = []
                for o in range(len(new_list)):
                    new_list_stores.append("missing")
                    o += 1
                # print('门店', new_list_stores)
                # print(len(new_list_stores))
            else:
                if stores_ != []:
                    stores_array = np.array(aa[stores_])
                    # 获取手机list

                    for i in range(len(stores_array)):
                        new_list_stores.append(stores_array[i, 0])
                    # print('门店', new_list_stores)
                    # print(len(new_list_stores))
                else:
                    pass


                # 打印出模糊匹配的字符：
            onestr = "岗位"
            jobs_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            # print(phone_)
            if jobs_ == []:
                o = 0
                new_list_jobs = []
                for o in range(len(new_list)):
                    new_list_jobs.append("missing")
                    o += 1
                # print('岗位', new_list_jobs)
                # print(len(new_list_jobs))

            else:
                if jobs_ != []:
                    jobs_array = np.array(aa[jobs_])
                    # 获取手机list

                    for i in range(len(jobs_array)):
                        new_list_jobs.append(jobs_array[i, 0])
                    # print('岗位', new_list_jobs)
                    # print(len(new_list_jobs))
                else:
                    pass

            # 打印出模糊匹配的字符：
            onestr = "人员状态"
            ryzt_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if ryzt_ == []:
                o = 0
                new_list_ryzt = []
                for o in range(len(new_list)):
                    new_list_ryzt.append("missing")
                    o += 1
            else:
                if ryzt_ != []:
                    ryzt_array = np.array(aa[ryzt_])
                    for i in range(len(ryzt_array)):
                        new_list_ryzt.append(ryzt_array[i, 0])
                else:
                    pass


            # 打印出模糊匹配的字符：
            onestr = "职位"
            zhiwei_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if zhiwei_ == []:
                o = 0
                new_list_zhiwei = []
                for o in range(len(new_list)):
                    new_list_zhiwei.append("missing")
                    o += 1
            else:
                if zhiwei_ != []:
                    zhiwei_array = np.array(aa[zhiwei_])
                    for i in range(len(zhiwei_array)):
                        new_list_zhiwei.append(zhiwei_array[i, 0])
                else:
                    pass

            # 打印出模糊匹配的字符：
            onestr = "应发工资"
            yfgz_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if yfgz_ == []:
                o = 0
                new_list_yfgz = []
                for o in range(len(new_list)):
                    new_list_yfgz.append("missing")
                    o += 1
            else:
                if yfgz_ != []:
                    yfgz_array = np.array(aa[yfgz_])
                    for i in range(len(yfgz_array)):
                        new_list_yfgz.append(yfgz_array[i, 0])
                else:
                    pass

            # 打印出模糊匹配的字符：
            onestr = "税后工资"
            shgz_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if shgz_ == []:
                o = 0
                new_list_shgz = []
                for o in range(len(new_list)):
                    new_list_shgz.append("missing")
                    o += 1
            else:
                if shgz_ != []:
                    shgz_array = np.array(aa[shgz_])
                    for i in range(len(shgz_array)):
                        new_list_shgz.append(shgz_array[i, 0])
                else:
                    pass

            # 打印出模糊匹配的字符：
            onestr = "实发工资"
            shifa_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if shifa_ == []:
                o = 0
                new_list_shifa = []
                for o in range(len(new_list)):
                    new_list_shifa.append("missing")
                    o += 1
            else:
                if shifa_ != []:
                    shifa_array = np.array(aa[shifa_])
                    for i in range(len(shifa_array)):
                        new_list_shifa.append(shifa_array[i, 0])
                else:
                    pass


            # 打印出模糊匹配的字符：
            onestr = "基本工资"
            jbgz_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if jbgz_ == []:
                o = 0
                new_list_jbgz = []
                for o in range(len(new_list)):
                    new_list_jbgz.append("missing")
                    o += 1
            else:
                if jbgz_ != []:
                    jbgz_array = np.array(aa[jbgz_])
                    for i in range(len(jbgz_array)):
                        new_list_jbgz.append(jbgz_array[i, 0])
                else:
                    pass


            # 打印出模糊匹配的字符：
            onestr = "全勤奖"
            qqj_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if qqj_ == []:
                o = 0
                new_list_qqj = []
                for o in range(len(new_list)):
                    new_list_qqj.append("missing")
                    o += 1
            else:
                if qqj_ != []:
                    qqj_array = np.array(aa[qqj_])
                    for i in range(len(qqj_array)):
                        new_list_qqj.append(qqj_array[i, 0])
                else:
                    pass

            # 打印出模糊匹配的字符：
            onestr = "固定工资总额"
            gdgzze_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if gdgzze_ == []:
                o = 0
                new_list_gdgzze = []
                for o in range(len(new_list)):
                    new_list_gdgzze.append("missing")
                    o += 1
            else:
                if gdgzze_ != []:
                    gdgzze_array = np.array(aa[gdgzze_])
                    for i in range(len(gdgzze_array)):
                        new_list_gdgzze.append(gdgzze_array[i, 0])
                else:
                    pass

            # 打印出模糊匹配的字符：
            onestr = "工龄工资"
            nlgz_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if nlgz_ == []:
                o = 0
                new_list_nlgz = []
                for o in range(len(new_list)):
                    new_list_nlgz.append("missing")
                    o += 1
            else:
                if nlgz_ != []:
                    nlgz_array = np.array(aa[nlgz_])
                    for i in range(len(nlgz_array)):
                        new_list_nlgz.append(nlgz_array[i, 0])
                else:
                    pass


            # 打印出模糊匹配的字符：
            onestr = "各类津贴"
            gljt_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if gljt_ == []:
                o = 0
                new_list_gljt = []
                for o in range(len(new_list)):
                    new_list_gljt.append("missing")
                    o += 1
            else:
                if gljt_ != []:
                    gljt_array = np.array(aa[gljt_])
                    for i in range(len(gljt_array)):
                        new_list_gljt.append(gljt_array[i, 0])
                else:
                    pass


            # 打印出模糊匹配的字符：
            onestr = "餐补"
            cb_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if cb_ == []:
                o = 0
                new_list_cb = []
                for o in range(len(new_list)):
                    new_list_cb.append("missing")
                    o += 1
            else:
                if cb_ != []:
                    cb_array = np.array(aa[cb_])
                    for i in range(len(cb_array)):
                        new_list_cb.append(cb_array[i, 0])
                else:
                    pass

            # 打印出模糊匹配的字符：
            onestr = "缺勤扣款"
            qqkk_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if qqkk_ == []:
                o = 0
                new_list_qqkk = []
                for o in range(len(new_list)):
                    new_list_qqkk.append("missing")
                    o += 1
            else:
                if qqkk_ != []:
                    qqkk_array = np.array(aa[qqkk_])
                    for i in range(len(qqkk_array)):
                        new_list_qqkk.append(qqkk_array[i, 0])
                else:
                    pass


          # 打印出模糊匹配的字符：
            onestr = "浮动工资总额"
            fdgzze_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if fdgzze_ == []:
                o = 0
                new_list_fdgzze = []
                for o in range(len(new_list)):
                    new_list_fdgzze.append("missing")
                    o += 1
            else:
                if fdgzze_ != []:
                    fdgzze_array = np.array(aa[fdgzze_])
                    for i in range(len(fdgzze_array)):
                        new_list_fdgzze.append(fdgzze_array[i, 0])
                else:
                    pass


          # 打印出模糊匹配的字符：
            onestr = "预支工资"
            yzgz_ = findallneedstr().findstr(onestr, Now_N_sheets,Nexcels)
            if yzgz_ == []:
                o = 0
                new_list_yzgz = []
                for o in range(len(new_list)):
                    new_list_yzgz.append("missing")
                    o += 1
            else:
                if yzgz_ != []:
                    yzgz_array = np.array(aa[yzgz_])
                    for i in range(len(yzgz_array)):
                        new_list_yzgz.append(yzgz_array[i, 0])
                else:
                    pass

        else:
            pass


        #假如一个储存成dataframe表格
        c = {"nemes": new_list,"entry_data": new_list3,"departure": new_list1,"phone": new_list2,"License_type":
            new_list_cardtype,"License_num": new_list_cardnum,"sex": new_list_sex,"shougu_data": new_list_Hire,"stores":
            new_list_stores,"jobs":new_list_jobs,"ryzt":new_list_ryzt , "zw":new_list_zhiwei ,"yfgz": new_list_yfgz,
             "shgz":new_list_shgz,"sfgz":new_list_shifa,"jbgz":new_list_jbgz,"qqj":new_list_qqj,"gdgzze":new_list_gdgzze,"nlgz":new_list_nlgz,
             "gljt":new_list_gljt,"cb":new_list_cb,"qqkk":new_list_qqkk,"fdgzze":new_list_fdgzze, "yzgz":new_list_yzgz}
        newdata = pd.DataFrame(c)

        return newdata





