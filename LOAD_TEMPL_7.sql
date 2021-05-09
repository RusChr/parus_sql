SELECT T.BOOK_NUMB
      ,T.ACC_NUMB
      ,DECODE(T.ROW_NUM, 1, T.ADDR_TYPE, NULL) ADDR_TYPE
      ,DECODE(T.ROW_NUM, 1, T.STREET, NULL)    STREET
      ,DECODE(T.ROW_NUM, 1, T.HOUSE, NULL)     HOUSE
      ,DECODE(T.ROW_NUM, 1, T.BLOCK, NULL)     BLOCK
      ,DECODE(T.ROW_NUM, 1, T.BUILD, NULL)     BUILD
      ,DECODE(T.ROW_NUM, 1, T.FLAT, NULL)      FLAT
      -- debug
      /*,T.ACC_RN
      ,T.LAND_AGENT
      ,T.HABI_AGENT
      ,T.MEMBS_AGENT*/
      --
      ,T.OWNER_FAMILYNAME
      ,T.OWNER_FIRSTNAME
      ,T.OWNER_LASTNAME
      ,DECODE(NVL(T.LAND_AGENT, T.HABI_AGENT), T.MEMBS_AGENT, NULL, T.MEMB_FAMILYNAME) MEMB_FAMILYNAME
      ,DECODE(NVL(T.LAND_AGENT, T.HABI_AGENT), T.MEMBS_AGENT, NULL, T.MEMB_FIRSTNAME)  MEMB_FIRSTNAME
      ,DECODE(NVL(T.LAND_AGENT, T.HABI_AGENT), T.MEMBS_AGENT, NULL, T.MEMB_LASTNAME)   MEMB_LASTNAME
      ,NULL
      ,T.HEAD_YN
      ,T.PASS_TYPE
      ,T.PASS_SER
      ,T.PASS_NUMB
      ,T.PASS_WHO
      ,T.PASS_WHEN
      ,T.PASS_DEPART
      ,T.DEAD_YN
      ,T.PHONE
      ,T.PRRELDEG
      ,T.AGNBURN
      ,T.REG_COUNTRY
      ,T.REG_REGION
      ,T.REG_DISTRICT
      ,T.REG_CITY
      ,T.REG_LOCALITY
      ,T.REG_ADDR_TYPE
      ,T.REG_STREET
      ,T.REG_HOUSE
      ,T.REG_BLOCK
      ,T.REG_BUILDING
      ,T.REG_FLAT
      ,T.ADDR_BURN
      ,T.LAND_CAD_NUMB          LAND_CAD_NUMB
      ,T.LAND_SQUARE            LAND_SQUARE
      ,T.LAND_CAD_COST          LAND_CAD_COST
      ,T.LAND_RTDOC_B_NAME      LAND_RTDOC_B_NAME
      ,T.LAND_RTDOC_B_NUMB      LAND_RTDOC_B_NUMB
      ,T.LAND_RTDOC_B_DATE      LAND_RTDOC_B_DATE
      ,T.LAND_RTDOC_R_NAME      LAND_RTDOC_R_NAME
      ,T.LAND_RTDOC_R_PREF      LAND_RTDOC_R_PREF
      ,T.LAND_RTDOC_R_NUMB      LAND_RTDOC_R_NUMB
      ,T.LAND_RIGHT_DATE        LAND_RIGHT_DATE
      ,T.LAND_PART_STR          LAND_PART_STR
      ,T.LAND_FORMUSE           LAND_FORMUSE
      ,T.HABI_CAD_NUMB          HABI_CAD_NUMB
      ,T.HABI_CAD_ROOM_NUMB     HABI_CAD_ROOM_NUMB
      ,T.HABI_CAD_COST          HABI_CAD_COST
      ,T.HABI_RTDOC_B_NAME      HABI_RTDOC_B_NAME
      ,T.HABI_RTDOC_B_NUMB      HABI_RTDOC_B_NUMB
      ,T.HABI_RTDOC_B_DATE      HABI_RTDOC_B_DATE
      ,T.HABI_RTDOC_R_NAME      HABI_RTDOC_R_NAME
      ,T.HABI_RTDOC_R_PREF      HABI_RTDOC_R_PREF
      ,T.HABI_RTDOC_R_NUMB      HABI_RTDOC_R_NUMB
      ,T.HABI_RIGHT_DATE        HABI_RIGHT_DATE
      ,T.SQUARE_ALL             SQUARE_ALL
      ,T.HABI_PART_STR          HABI_PART_STR
      ,T.HOUSING


  FROM (SELECT ACC.RN                 ACC_RN
              ,LHM.PRN                LHM_PRN
              ,LHM.LAND_AGENT         LAND_AGENT
              ,LHM.HABI_AGENT         HABI_AGENT
              ,LHM.MEMBS_AGENT        MEMBS_AGENT
              ,ROW_NUMBER() OVER(PARTITION BY BOOK.NUMB, ACC.NUMB
                                     ORDER BY BOOK.NUMB, ACC.NUMB,
                                              (SELECT A.AGNFAMILYNAME FROM AGNLIST A WHERE A.RN = NVL(LHM.LAND_AGENT, LHM.HABI_AGENT)),
                                              (SELECT A.AGNFIRSTNAME FROM AGNLIST A WHERE A.RN = NVL(LHM.LAND_AGENT, LHM.HABI_AGENT)),
                                              (SELECT A.AGNFAMILYNAME FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT),
                                              (SELECT A.AGNFIRSTNAME FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT),
                                              LHM.HEAD_YN) ROW_NUM
              ,TRIM(BOOK.NUMB)                             BOOK_NUMB
              ,TRIM(ACC.NUMB)                              ACC_NUMB
              ,(SELECT G.SLOCALITYKIND FROM V_GEOGRAFY G WHERE G.NRN = ACC.GEOGRAFY)                                                          ADDR_TYPE
              ,(SELECT G.SGEOGRNAME FROM V_GEOGRAFY G WHERE G.SGEOGRTYPE = 5 START WITH G.NRN = ACC.GEOGRAFY CONNECT BY PRIOR G.NPRN = G.NRN) STREET
              ,ACC.HOUSE
              ,ACC.BLOCK
              ,ACC.BUILD
              ,ACC.FLAT
              ,(SELECT A.AGNFAMILYNAME FROM AGNLIST A WHERE A.RN = NVL(LHM.LAND_AGENT, LHM.HABI_AGENT)) OWNER_FAMILYNAME
              ,(SELECT A.AGNFIRSTNAME FROM AGNLIST A WHERE A.RN = NVL(LHM.LAND_AGENT, LHM.HABI_AGENT))  OWNER_FIRSTNAME
              ,(SELECT A.AGNLASTNAME FROM AGNLIST A WHERE A.RN = NVL(LHM.LAND_AGENT, LHM.HABI_AGENT))   OWNER_LASTNAME
              ,(SELECT A.AGNFAMILYNAME FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT)                     MEMB_FAMILYNAME
              ,(SELECT A.AGNFIRSTNAME FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT)                      MEMB_FIRSTNAME
              ,(SELECT A.AGNLASTNAME FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT)                       MEMB_LASTNAME
              ,NULL -- Зарегистрирован но не проживает, да (оставить пустым)
              ,LHM.HEAD_YN
              ,(SELECT NAME FROM AGNPASSTYPE P WHERE P.RN = (SELECT PASSPORT_TYPE FROM AGNLIST A WHERE A.RN = NVL(LHM.MEMBS_AGENT, LHM.HABI_AGENT)))  PASS_TYPE
              ,(SELECT PASSPORT_SER FROM AGNLIST A WHERE A.RN = NVL(LHM.MEMBS_AGENT, LHM.HABI_AGENT))          PASS_SER
              ,(SELECT PASSPORT_NUMB FROM AGNLIST A WHERE A.RN = NVL(LHM.MEMBS_AGENT, LHM.HABI_AGENT))         PASS_NUMB
              ,(SELECT PASSPORT_WHO FROM AGNLIST A WHERE A.RN = NVL(LHM.MEMBS_AGENT, LHM.HABI_AGENT))          PASS_WHO
              ,(SELECT PASSPORT_WHEN FROM AGNLIST A WHERE A.RN = NVL(LHM.MEMBS_AGENT, LHM.HABI_AGENT))         PASS_WHEN
              ,(SELECT PASSPORT_DEPART FROM AGNLIST A WHERE A.RN = NVL(LHM.MEMBS_AGENT, LHM.HABI_AGENT))       PASS_DEPART
              ,(SELECT DECODE(M.NDEAD, 1, 'Да', 0, 'Нет') FROM V_MSUACCMEMB M WHERE M.NRN = LHM.MEMBS_RN)      DEAD_YN
              ,(SELECT PHONE FROM AGNLIST A WHERE A.RN = NVL(LHM.MEMBS_AGENT, LHM.HABI_AGENT))                 PHONE
              ,(SELECT NAME FROM PRRELDEG P WHERE P.RN = LHM.PRRELDEG)                                         PRRELDEG
              ,(SELECT AGNBURN FROM AGNLIST A WHERE A.RN = NVL(LHM.MEMBS_AGENT, LHM.HABI_AGENT))               AGNBURN
              ,(SELECT G.SGEOGRNAME FROM V_GEOGRAFY G 
                 WHERE G.SGEOGRTYPE = 1 START WITH G.NRN = (SELECT GEOGRAFY_RN FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT) CONNECT BY PRIOR G.NPRN = G.NRN) REG_COUNTRY
              ,(SELECT G.SGEOGRNAME FROM V_GEOGRAFY G 
                 WHERE G.SGEOGRTYPE = 2 START WITH G.NRN = (SELECT GEOGRAFY_RN FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT) CONNECT BY PRIOR G.NPRN = G.NRN) REG_REGION
              ,(SELECT G.SGEOGRNAME FROM V_GEOGRAFY G 
                 WHERE G.SGEOGRTYPE = 3 START WITH G.NRN = (SELECT GEOGRAFY_RN FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT) CONNECT BY PRIOR G.NPRN = G.NRN) REG_DISTRICT
              ,(SELECT G.SGEOGRNAME FROM V_GEOGRAFY G 
                 WHERE G.SGEOGRTYPE = 8 START WITH G.NRN = (SELECT GEOGRAFY_RN FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT) CONNECT BY PRIOR G.NPRN = G.NRN) REG_CITY
              ,(SELECT G.SGEOGRNAME FROM V_GEOGRAFY G 
                 WHERE G.SGEOGRTYPE = 4 START WITH G.NRN = (SELECT GEOGRAFY_RN FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT) CONNECT BY PRIOR G.NPRN = G.NRN) REG_LOCALITY
              ,(SELECT G.SLOCALITYKIND FROM V_GEOGRAFY G WHERE G.NRN = (SELECT GEOGRAFY_RN FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT))                     REG_ADDR_TYPE
              ,(SELECT G.SGEOGRNAME FROM V_GEOGRAFY G 
                 WHERE G.SGEOGRTYPE = 5 START WITH G.NRN = (SELECT GEOGRAFY_RN FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT) CONNECT BY PRIOR G.NPRN = G.NRN) REG_STREET
              ,(SELECT A.ADDR_HOUSE FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT)                         REG_HOUSE
              ,(SELECT A.ADDR_BLOCK FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT)                         REG_BLOCK
              ,(SELECT A.ADDR_BUILDING FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT)                      REG_BUILDING
              ,(SELECT A.ADDR_FLAT FROM AGNLIST A WHERE A.RN = LHM.MEMBS_AGENT)                          REG_FLAT
              ,(SELECT A.ADDR_BURN FROM AGNLIST A WHERE A.RN = NVL(LHM.MEMBS_AGENT, LHM.HABI_AGENT))     ADDR_BURN
              ,(SELECT ML.CAD_NUMB FROM MSULAND ML WHERE ML.RN = LHM.LPRN)                               LAND_CAD_NUMB
              ,(SELECT ML.SQUARE FROM MSULAND ML WHERE ML.RN = LHM.LPRN)                                 LAND_SQUARE
              ,(SELECT ML.CAD_COST FROM MSULAND ML WHERE ML.RN = LHM.LPRN)                               LAND_CAD_COST
              ,(SELECT NAME FROM MSURTDOC DOC WHERE DOC.RN = LHM.LAND_RTDOC_B)                           LAND_RTDOC_B_NAME
              ,LHM.LAND_RTDOC_B_NUMB                                                                     LAND_RTDOC_B_NUMB
              ,LHM.LAND_RTDOC_B_DATE                                                                     LAND_RTDOC_B_DATE
              ,(SELECT NAME FROM MSURTDOC DOC WHERE DOC.RN = LHM.LAND_RTDOC_R)                           LAND_RTDOC_R_NAME
              ,LHM.LAND_RTDOC_R_PREF                                                                     LAND_RTDOC_R_PREF
              ,LHM.LAND_RTDOC_R_NUMB                                                                     LAND_RTDOC_R_NUMB
              ,LHM.LAND_RIGHT_DATE                                                                       LAND_RIGHT_DATE
              ,LHM.LAND_PART_STR                                                                         LAND_PART_STR
              ,(SELECT VML.SFORMUSE_NAME FROM V_MSULAND VML WHERE VML.NRN = LHM.LPRN)                    LAND_FORMUSE
              ,(SELECT MH.CAD_NUMB FROM MSUHABI MH WHERE MH.RN = LHM.HPRN)                               HABI_CAD_NUMB
              ,(SELECT MH.CAD_ROOM_NUMB FROM MSUHABI MH WHERE MH.RN = LHM.HPRN)                          HABI_CAD_ROOM_NUMB
              ,(SELECT MH.CAD_COST FROM MSUHABI MH WHERE MH.RN = LHM.HPRN)                               HABI_CAD_COST
              ,(SELECT NAME FROM MSURTDOC DOC WHERE DOC.RN = LHM.LAND_RTDOC_B)                           HABI_RTDOC_B_NAME
              ,LHM.HABI_RTDOC_B_NUMB                                                                     HABI_RTDOC_B_NUMB
              ,LHM.HABI_RTDOC_B_DATE                                                                     HABI_RTDOC_B_DATE
              ,(SELECT NAME FROM MSURTDOC DOC WHERE DOC.RN = LHM.HABI_RTDOC_R)                           HABI_RTDOC_R_NAME
              ,LHM.HABI_RTDOC_R_PREF                                                                     HABI_RTDOC_R_PREF
              ,LHM.HABI_RTDOC_R_NUMB                                                                     HABI_RTDOC_R_NUMB
              ,LHM.HABI_RIGHT_DATE                                                                       HABI_RIGHT_DATE
              ,(SELECT MH.SQUARE_ALL FROM MSUHABI MH WHERE MH.RN = LHM.HPRN)                             SQUARE_ALL
              ,LHM.HABI_PART_STR                                                                         HABI_PART_STR
              ,NULL                                                                                      HOUSING


          FROM MSUACC     ACC
              ,MSUBOOK    BOOK
              
              ,(SELECT NVL(NVL(LAND.PRN, HABI.PRN), MEMBS.PRN) PRN
                      ,LAND.AGENT                              LAND_AGENT
                      ,HABI.AGENT                              HABI_AGENT
                      ,MEMBS.AGENT                             MEMBS_AGENT
                      ,LAND.LPRN
                      ,HABI.HPRN
                      ,MEMBS.RN                                MEMBS_RN
                      ,LAND.RTDOC_B                            LAND_RTDOC_B
                      ,LAND.RTDOC_B_NUMB                       LAND_RTDOC_B_NUMB
                      ,LAND.RTDOC_B_DATE                       LAND_RTDOC_B_DATE
                      ,LAND.RTDOC_R                            LAND_RTDOC_R
                      ,LAND.RTDOC_R_PREF                       LAND_RTDOC_R_PREF
                      ,LAND.RTDOC_R_NUMB                       LAND_RTDOC_R_NUMB
                      ,LAND.RIGHT_DATE                         LAND_RIGHT_DATE
                      ,LAND.PART_STR                           LAND_PART_STR
                      ,HABI.RTDOC_B                            HABI_RTDOC_B
                      ,HABI.RTDOC_B_NUMB                       HABI_RTDOC_B_NUMB
                      ,HABI.RTDOC_B_DATE                       HABI_RTDOC_B_DATE
                      ,HABI.RTDOC_R                            HABI_RTDOC_R
                      ,HABI.RTDOC_R_PREF                       HABI_RTDOC_R_PREF
                      ,HABI.RTDOC_R_NUMB                       HABI_RTDOC_R_NUMB
                      ,HABI.RIGHT_DATE                         HABI_RIGHT_DATE
                      ,HABI.PART_STR                           HABI_PART_STR
                      ,MEMBS.HEAD_YN
                      ,MEMBS.PRRELDEG
                     
                 FROM (SELECT LANDR.AGENT
                             ,ACCL.PRN
                             ,LANDR.PRN LPRN
                             ,LANDR.RTDOC_B
                             ,LANDR.RTDOC_B_NUMB
                             ,LANDR.RTDOC_B_DATE
                             ,LANDR.RTDOC_R
                             ,LANDR.RTDOC_R_PREF
                             ,LANDR.RTDOC_R_NUMB
                             ,LANDR.RIGHT_DATE
                             ,LANDR.PART||'/'||LANDR.PART_ALL PART_STR
                         FROM MSUACCLAND     ACCL
                             ,MSULANDRIGHT   LANDR
                        WHERE ACCL.LAND = LANDR.PRN
                      ) LAND
                      FULL OUTER JOIN
                      (SELECT HABIR.AGENT
                             ,ACCH.PRN
                             ,HABIR.PRN HPRN
                             ,HABIR.RTDOC_B
                             ,HABIR.RTDOC_B_NUMB
                             ,HABIR.RTDOC_B_DATE
                             ,HABIR.RTDOC_R
                             ,HABIR.RTDOC_R_PREF
                             ,HABIR.RTDOC_R_NUMB
                             ,HABIR.RIGHT_DATE
                             ,HABIR.RIGHT_SQUARE
                             ,HABIR.PART||'/'||HABIR.PART_ALL PART_STR
                         FROM MSUACCHABI     ACCH
                             ,MSUHABIRIGHT   HABIR
                             ,MSUHABI        MHABI
                        WHERE ACCH.HABI = HABIR.PRN
                          AND ACCH.HABI = MHABI.RN
                          AND MHABI.BASE_CHK = 1
                      ) HABI ON LAND.PRN = HABI.PRN AND LAND.AGENT = HABI.AGENT
                      FULL OUTER JOIN
                      (SELECT MEMB.AGENT
                             ,ACCM.PRN
                             ,ACCM.RN
                             ,DECODE(ACCM.HEAD, 1, 'Да', 0, 'Нет') HEAD_YN
                             ,ACCM.PRRELDEG
                         FROM MSUACCMEMB     ACCM
                             ,MSUMEMB        MEMB
                        WHERE ACCM.MEMB = MEMB.RN
                      ) MEMBS ON NVL(LAND.PRN, HABI.PRN) = MEMBS.PRN AND NVL(LAND.AGENT, HABI.AGENT) = MEMBS.AGENT
               ) LHM
                             
          
         WHERE ACC.BOOK = BOOK.RN
           AND ACC.RN = LHM.PRN(+)
           --AND ACC.RN IN (22600075, 22600103, 22600763, 22596839)
           AND ACC.CRN = 22596750) T

 ORDER BY 1
         ,2
         ,T.LHM_PRN
         ,OWNER_FAMILYNAME
         ,OWNER_FIRSTNAME
         ,MEMB_FAMILYNAME
         ,MEMB_FIRSTNAME
         ,T.HEAD_YN
