          .join(df_RLTNSHP.as("RLTNSHP"), $"RLTNSHP.ACCT_ID" === $"SCD.SCD_ORG_ID")


          .where(trim($"SCD.SCD_ORG_TYPE_CD")==="WAC")


          .where($"SCD.RLTD_SCD_ORG_TYPE_CD".isin("GRP","CLNT","ASSN"))


          .select(trim($"SCD.RLTD_SCD_ORG_SOR_CD").as("RLTD_SCD_ORG_SOR_CD"),trim($"SCD.RLTD_SCD_ORG_ID").as("RLTD_SCD_ORG_ID"),  trim($"SCD.SCD_ORG_ID").as("SCD_ORG_ID"),trim($"SCD.RLTD_SCD_ORG_TYPE_CD").as("RLTD_SCD_ORG_TYPE_CD"))


val FRAME4_1= df_wrk.as("F3").join(SCD_RLTNSHP_GRP.as("SCD1"),$"F3.SGMNTN_SOR_CD" === $"SCD1.RLTD_SCD_ORG_SOR_CD"
            && $"F3.SGMNTN_GRP_ID" === $"SCD1.RLTD_SCD_ORG_ID")


                                        .select(
                                        $"F3.SGMNTN_ASSN_ID"
                                       ,$"F3.SGMNTN_CLNT_ID"
                                       ,$"F3.SGMNTN_GRP_ID"
                                       ,$"F3.SGMNTN_SUBGRP_ID"
                                       ,$"F3.EMPLR_DEPT_NBR"
                                       ,$"F3.FULLY_INSRD_CD"
                                       ,$"F3.GRP_STTS_CD"
                                       ,$"F3.HLTH_PROD_SRVC_TYPE_CD"
                                       ,$"F3.prod_id1".as("prod_id")


val FRAME4_2= df_wrk.as("F3") .join(SCD_RLTNSHP_CLNT.as("SCD2"),$"F3.SGMNTN_SOR_CD" === $"SCD2.RLTD_SCD_ORG_SOR_CD"
            && $"F3.SGMNTN_CLNT_ID" === $"SCD2.RLTD_SCD_ORG_ID").select(
                                        $"F3.SGMNTN_ASSN_ID"
                                       ,$"F3.SGMNTN_CLNT_ID"
                                       ,$"F3.SGMNTN_GRP_ID"
                                       ,$"F3.SGMNTN_SUBGRP_ID"
                                       ,$"F3.EMPLR_DEPT_NBR"
                                       ,$"F3.FULLY_INSRD_CD"
                                       ,$"F3.GRP_STTS_CD"
                                       ,$"F3.HLTH_PROD_SRVC_TYPE_CD"
                                       ,$"F3.prod_id1".as("prod_id")


val FRAME4_3= df_wrk.as("F3").join(SCD_RLTNSHP_ASSN.as("SCD3"),$"F3.SGMNTN_SOR_CD" === $"SCD3.RLTD_SCD_ORG_SOR_CD"
            && $"F3.SGMNTN_ASSN_ID" === $"SCD3.RLTD_SCD_ORG_ID").select(
                                        $"F3.SGMNTN_ASSN_ID"
                                       ,$"F3.SGMNTN_CLNT_ID"
                                       ,$"F3.SGMNTN_GRP_ID"
                                       ,$"F3.SGMNTN_SUBGRP_ID"
                                       ,$"F3.EMPLR_DEPT_NBR"
                                       ,$"F3.FULLY_INSRD_CD"
                                       ,$"F3.GRP_STTS_CD"
                                       ,$"F3.HLTH_PROD_SRVC_TYPE_CD"
                                       ,$"F3.prod_id1".as("prod_id")


val FRAME5= FRAME4.as("F4").join(broadcast(scd_org).as("srg"),$"F4.ACCT_ID"=== $"srg.ACCT_ID","left_outer")


					.select($"F4.ACCT_ID"
	                          ,$"F4.SGMNTN_ASSN_ID"
	                          ,$"F4.SGMNTN_CLNT_ID"
	                          ,$"F4.SGMNTN_GRP_ID"
	                          ,$"F4.SGMNTN_SUBGRP_ID"
	                          ,$"F4.EMPLR_DEPT_NBR"
	                          ,$"F4.FULLY_INSRD_CD"
	                          ,$"F4.GRP_STTS_CD"
	                          ,$"F4.HLTH_PROD_SRVC_TYPE_CD"
                            ,$"ORIG_HLTH_PROD_SRVC_TYPE_CD"
	                          ,when($"F4.prod_id".isin("","~01","~02","~03","~NA","~DV","UNK") || $"F4.prod_id".isNull,"NA").otherwise(upper(trim(regexp_replace($"F4.prod_id","\\^", "\\s")))).as("prod_id")


  val frame5_new =CIIUtilities.readDFfromS3(aDataSrcCfg,"frame5")