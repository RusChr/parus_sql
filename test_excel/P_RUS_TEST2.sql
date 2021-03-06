CREATE OR REPLACE PROCEDURE P_RUS_TEST2(NCOMPANY IN NUMBER) AS
	/* ????????? ????? */
	SHEET_NAME   CONSTANT PKG_STD.TSTRING := '????1'; -- ??? ?????-???????
	CELL_COMPANY CONSTANT PKG_STD.TSTRING := 'organiz'; -- ???????????? ???????????
	-- ??????
	HEADER1      CONSTANT PKG_STD.TSTRING := 'rus_header';
	AGN          CONSTANT PKG_STD.TSTRING := 'rus_agents'; -- ???????????
	AGNABBR      CONSTANT PKG_STD.TSTRING := 'rus_agent_code'; -- ???????? ???????????
	AGNNAME      CONSTANT PKG_STD.TSTRING := 'rus_agent_name'; -- ???????????? ???????????
	/* ?????????? */
	SCOMPANY     VARCHAR2(100);
	ILINE_INDEX  INTEGER;
BEGIN
	SCOMPANY := GET_COMPANY_FULLNAME(0, NCOMPANY);
	/* ?????? */
	PRSG_EXCEL.PREPARE;
	/* ????????? ???????? ???????? ????? */
	PRSG_EXCEL.SHEET_SELECT(SHEET_NAME);
	/* ???????? */
	PRSG_EXCEL.CELL_DESCRIBE(CELL_COMPANY);

	PRSG_EXCEL.LINE_DESCRIBE(HEADER1);
	PRSG_EXCEL.LINE_DESCRIBE(AGN);
	PRSG_EXCEL.LINE_CELL_DESCRIBE(AGN, AGNABBR);
	PRSG_EXCEL.LINE_CELL_DESCRIBE(AGN, AGNNAME);

	/* ?????? ???????? */
	PRSG_EXCEL.CELL_VALUE_WRITE(CELL_COMPANY, SCOMPANY);

	FOR TREC IN (SELECT AGNABBR
										 ,AGNNAME
								 FROM V_AGNLIST
								WHERE ROWNUM < 50) LOOP
	
		/* ?????????? ?????? ??????????? */
		ILINE_INDEX := PRSG_EXCEL.LINE_CONTINUE(AGN);
	
		/* ?????? ???????? */
		PRSG_EXCEL.CELL_VALUE_WRITE(AGNABBR, 0, ILINE_INDEX, TREC.AGNABBR);
		PRSG_EXCEL.CELL_VALUE_WRITE(AGNNAME, 0, ILINE_INDEX, TREC.AGNNAME);
	
	END LOOP;

	PRSG_EXCEL.LINE_DELETE(AGN);

END;
