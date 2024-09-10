package com.anthem.etl.cii.dao

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import com.anthem.etl.cii.util._
import org.apache.spark.storage.StorageLevel
import com.anthem.etl.dao._
import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.functions.upper
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions.current_timestamp
import java.net.URI
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

class WorkAccntSgmntXwalkDAO extends DataSFAccessObject  {

override def importData(aDataSrcCfg: DataSourceSFConfig, aSchemaDef: StructType): DataFrame = {
val sqlContext = getSparkSession.sqlContext
import sqlContext.implicits._
val env = aDataSrcCfg.env
val release = aDataSrcCfg.release
val layer = aDataSrcCfg.layer
//val dbname = s"${env}_pcaciirph_nogbd_${release}_${layer}"
val dbname_gbd = s"${env}_ciievolve"
    val dbname = s"${env}_ciievolve"
    val dbname_cdl = s"${env}_ciievolve"
    val dbname_adhoc = s"${env}_ciievolve_adhoc_cons"
var sqls = Array[String]()

aDataSrcCfg.queryMap.asScala foreach (sqlFileMap => {
sqls = sqlFileMap._2.split(",")
})


val df_RLTNSHP = broadcast(getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(0))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)))
// segmentation will add    PRMRY_CVRG_CD & MEDCR_CD logic here before acct_id is derived--> LOOK ABOVE
val df_wrk= getSparkSession.table(s"${dbname}.wrk_sgmntn_mbr")

val SCD_RLTNSHP= getSparkSession.table(s"${dbname}.SCD_ORG_RLTNSHP").as("SCD")
          .join(df_RLTNSHP.as("RLTNSHP"), $"RLTNSHP.ACCT_ID" === $"SCD.SCD_ORG_ID")
          .where(trim($"SCD.SCD_ORG_TYPE_CD")==="WAC")
          .where($"SCD.RLTD_SCD_ORG_TYPE_CD".isin("GRP","CLNT","ASSN"))
          .select(trim($"SCD.RLTD_SCD_ORG_SOR_CD").as("RLTD_SCD_ORG_SOR_CD"),trim($"SCD.RLTD_SCD_ORG_ID").as("RLTD_SCD_ORG_ID"),  trim($"SCD.SCD_ORG_ID").as("SCD_ORG_ID"),trim($"SCD.RLTD_SCD_ORG_TYPE_CD").as("RLTD_SCD_ORG_TYPE_CD"))


        val  SCD_RLTNSHP_GRP=broadcast(SCD_RLTNSHP.filter($"RLTD_SCD_ORG_TYPE_CD"=== "GRP"))
         val  SCD_RLTNSHP_CLNT=broadcast(SCD_RLTNSHP.filter($"RLTD_SCD_ORG_TYPE_CD"=== "CLNT"))
          val  SCD_RLTNSHP_ASSN=broadcast(SCD_RLTNSHP.filter($"RLTD_SCD_ORG_TYPE_CD"=== "ASSN"))


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
                                       ,$"F3.PLAN_ID"
                                       ,$"F3.SGMNTN_SOR_CD"
                                       ,$"SCD1.SCD_ORG_ID".as("ACCT_ID")
									   //,$"F3.PRCHSR_ORG_NBR" //
                      ,$"F3.RLTD_PRCHSR_ORG_NBR"
                      ,$"F3.prod_id".as("prod_id_fltr")
                      ,$"F3.BNFT_PKG_ID"
                      ,$"F3.PKG_NBR"
                      ,$"F3.MBRSHP_SOR_CD"
                      ,$"F3.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"F3.PROD_SOR_CD"
                      ,$"F3.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"F3.MBR_KEY"
                      ,$"F3.BNFT_PKG_KEY"
                      ,$"F3.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"F3.FUNDG_CF_CD"
					  ,$"F3.src_grp_nbr"
					  ,$"F3.mdcl_mbr_cvrg_cnt"
					  ,$"F3.vsn_mbr_cnt"
					  ,$"F3.dntl_mbr_cnt"
					  ,$"F3.mbr_prod_enrlmnt_trmntn_dt"
					  ,$"F3.cntrct_type_cd"
					  ,$"F3.othr_insrnc_type_cd"
					  ,$"F3.gndr_cd"
					  ,$"F3.mbr_mnth_cob_cd"
					  ,$"F3.prcp_id"
					  ,$"F3.prcp_ctgry_cd"
					  ,$"F3.prcp_type_cd"
					  ,$"F3.hc_id"
					  ,$"F3.sbscrbr_id"
					  ,$"F3.scrty_lvl_cd"
					  ,$"F3.prod_ofrg_key"
					  ,$"F3.mdcl_expsr_nbr"
					  ,$"F3.phrmcy_mbr_expsr_nbr"
					  ,$"F3.dntl_expsr_nbr"
					  ,$"F3.vsn_expsr_nbr"
					  ,$"F3.MAX_FUNDG_CF_CD"//
						  ,$"F3.PHRMCY_MBR_CVRG_CNT"
						  ,$"F3.RX_OVERRIDE_FLAG"
						  ,$"F3.ORIG_HLTH_PROD_SRVC_TYPE_CD"
						  ,$"F3.MBU_CF_CD"
              ,$"F3.CLM_CD"
              ,$"F3.CLM_RPTG_1_CD"
              ,$"F3.CLM_RPTG_2_CD"
              ,$"F3.CLM_RPTG_3_CD"
              ,$"F3.EMP_NBR"
              ,$"F3.PLAN_GRP_ID"
              ,$"F3.CDHP_CTGRY_CD"
              ,$"F3.MBR_NTWK_ID"
              ,$"F3.PRMRY_CVRG_CD"
              ,$"F3.MEDCR_CD")


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
                                       ,$"F3.PLAN_ID"
                                       ,$"F3.SGMNTN_SOR_CD"
                                       ,$"SCD2.SCD_ORG_ID".as("ACCT_ID")
									   //,$"F3.PRCHSR_ORG_NBR" //
                      ,$"F3.RLTD_PRCHSR_ORG_NBR"
                      ,$"F3.prod_id".as("prod_id_fltr")
                      ,$"F3.BNFT_PKG_ID"
                      ,$"F3.PKG_NBR"
                      ,$"F3.MBRSHP_SOR_CD"
                      ,$"F3.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"F3.PROD_SOR_CD"
                      ,$"F3.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"F3.MBR_KEY"
                      ,$"F3.BNFT_PKG_KEY"
                      ,$"F3.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"F3.FUNDG_CF_CD"
					  ,$"F3.src_grp_nbr"
					  ,$"F3.mdcl_mbr_cvrg_cnt"
					  ,$"F3.vsn_mbr_cnt"
					  ,$"F3.dntl_mbr_cnt"
					  ,$"F3.mbr_prod_enrlmnt_trmntn_dt"
					  ,$"F3.cntrct_type_cd"
					  ,$"F3.othr_insrnc_type_cd"
					  ,$"F3.gndr_cd"
					  ,$"F3.mbr_mnth_cob_cd"
					  ,$"F3.prcp_id"
					  ,$"F3.prcp_ctgry_cd"
					  ,$"F3.prcp_type_cd"
					  ,$"F3.hc_id"
					  ,$"F3.sbscrbr_id"
					  ,$"F3.scrty_lvl_cd"
					  ,$"F3.prod_ofrg_key"
					  ,$"F3.mdcl_expsr_nbr"
					  ,$"F3.phrmcy_mbr_expsr_nbr"
					  ,$"F3.dntl_expsr_nbr"
					  ,$"F3.vsn_expsr_nbr"
					  ,$"F3.MAX_FUNDG_CF_CD"//
						  ,$"F3.PHRMCY_MBR_CVRG_CNT"
						  ,$"F3.RX_OVERRIDE_FLAG"
						  ,$"F3.ORIG_HLTH_PROD_SRVC_TYPE_CD"
						  ,$"F3.MBU_CF_CD"
            ,$"F3.CLM_CD"
            ,$"F3.CLM_RPTG_1_CD"
            ,$"F3.CLM_RPTG_2_CD"
            ,$"F3.CLM_RPTG_3_CD"
            ,$"F3.EMP_NBR"
            ,$"F3.PLAN_GRP_ID"
            ,$"F3.CDHP_CTGRY_CD"
            ,$"F3.MBR_NTWK_ID"
            ,$"F3.PRMRY_CVRG_CD"
              ,$"F3.MEDCR_CD")

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
                                       ,$"F3.PLAN_ID"
                                       ,$"F3.SGMNTN_SOR_CD"
                                       ,$"SCD3.SCD_ORG_ID".as("ACCT_ID")
									   //,$"F3.PRCHSR_ORG_NBR" //
                      ,$"F3.RLTD_PRCHSR_ORG_NBR"
                      ,$"F3.prod_id".as("prod_id_fltr")
                      ,$"F3.BNFT_PKG_ID"
                      ,$"F3.PKG_NBR"
                      ,$"F3.MBRSHP_SOR_CD"
                      ,$"F3.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"F3.PROD_SOR_CD"
                      ,$"F3.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"F3.MBR_KEY"
                      ,$"F3.BNFT_PKG_KEY"
                      ,$"F3.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"F3.FUNDG_CF_CD"
					  ,$"F3.src_grp_nbr"
					  ,$"F3.mdcl_mbr_cvrg_cnt"
					  ,$"F3.vsn_mbr_cnt"
					  ,$"F3.dntl_mbr_cnt"
					  ,$"F3.mbr_prod_enrlmnt_trmntn_dt"
					  ,$"F3.cntrct_type_cd"
					  ,$"F3.othr_insrnc_type_cd"
					  ,$"F3.gndr_cd"
					  ,$"F3.mbr_mnth_cob_cd"
					  ,$"F3.prcp_id"
					  ,$"F3.prcp_ctgry_cd"
					  ,$"F3.prcp_type_cd"
					  ,$"F3.hc_id"
					  ,$"F3.sbscrbr_id"
					  ,$"F3.scrty_lvl_cd"
					  ,$"F3.prod_ofrg_key"
					  ,$"F3.mdcl_expsr_nbr"
					  ,$"F3.phrmcy_mbr_expsr_nbr"
					  ,$"F3.dntl_expsr_nbr"
					  ,$"F3.vsn_expsr_nbr"
					  ,$"F3.MAX_FUNDG_CF_CD"//
						  ,$"F3.PHRMCY_MBR_CVRG_CNT"
						  ,$"F3.RX_OVERRIDE_FLAG"
						  ,$"F3.ORIG_HLTH_PROD_SRVC_TYPE_CD"
						  ,$"F3.MBU_CF_CD"
            ,$"F3.CLM_CD"
            ,$"F3.CLM_RPTG_1_CD"
            ,$"F3.CLM_RPTG_2_CD"
            ,$"F3.CLM_RPTG_3_CD"
            ,$"F3.EMP_NBR"
            ,$"F3.PLAN_GRP_ID"
            ,$"F3.CDHP_CTGRY_CD"
            ,$"F3.MBR_NTWK_ID"
            ,$"F3.PRMRY_CVRG_CD"
             ,$"F3.MEDCR_CD")

val FRAME4 =  FRAME4_1.unionAll(FRAME4_2).unionAll(FRAME4_3)

val scd_org= getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(1))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)) //scd_org_scrty.sql

  //segmentation -- cleaning prod_id, bnft_pkg_id and pkg_nbr to remove carrot at the end of derivation

val FRAME5= FRAME4.as("F4").join(broadcast(scd_org).as("srg"),$"F4.ACCT_ID"=== $"srg.ACCT_ID","left_outer")
            .withColumn("RPTG_OFSHR_ACSBL_IND",when($"srg.ACCT_ID".isNotNull,$"srg.RPTG_OFSHR_ACSBL_IND").otherwise(lit("N")))
            .withColumn("SCRTY_LVL_CD1",when($"srg.ACCT_ID".isNotNull,$"srg.SCRTY_LVL_CD").otherwise(lit("N")))
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
	                          ,$"F4.PLAN_ID"
	                          ,$"F4.SGMNTN_SOR_CD"
	                         ,$"RPTG_OFSHR_ACSBL_IND"
	                          ,$"SCRTY_LVL_CD1".as("SCRTY_LVL_CD")
	                        ,$"mbr_key"
							,$"elgblty_clndr_mnth_end_dt"
							,$"mbrshp_sor_cd"
							,$"src_grp_nbr"
							,$"rltd_prchsr_org_nbr"
							,$"SGMNTN_GRP_ID".as("prchsr_org_nbr")
							,$"fundg_cf_cd"
							,$"prod_sor_cd"
							,$"prod_id_fltr"
							,when($"bnft_pkg_id".isin("","~01","~02","~03","~NA","~DV","UNK") || $"bnft_pkg_id".isNull,"NA").otherwise(upper(trim(regexp_replace($"bnft_pkg_id","\\^", "\\s")))).as("bnft_pkg_id")
							,when($"pkg_nbr".isin("","~01","~02","~03","~NA","~DV","UNK") || $"pkg_nbr".isNull,"NA").otherwise(upper(trim(regexp_replace($"pkg_nbr","\\^", "\\s")))).as("pkg_nbr")
							,$"mdcl_mbr_cvrg_cnt"
							,$"vsn_mbr_cnt"
							,$"dntl_mbr_cnt"
							,$"mbr_prod_enrlmnt_efctv_dt"
							,$"mbr_prod_enrlmnt_trmntn_dt"
							,$"cntrct_type_cd"
							,$"othr_insrnc_type_cd"
							,$"gndr_cd"
							,$"mbr_mnth_cob_cd"
							,$"prcp_id"
							,$"prcp_ctgry_cd"
							,$"bnft_pkg_key"
							,$"rltd_prchsr_org_type_cd"
							,$"prcp_type_cd"
							,$"hc_id"
							,$"sbscrbr_id"
							,$"SCRTY_LVL_CD1".as("scrty_lvl_cd_fltr")
							,$"prod_ofrg_key"
							,$"mdcl_expsr_nbr"
							,$"phrmcy_mbr_expsr_nbr"
							,$"dntl_expsr_nbr"
							,$"vsn_expsr_nbr"
							,$"MAX_FUNDG_CF_CD"
							,$"PHRMCY_MBR_CVRG_CNT"
							,$"hc_id".as("hcid_key")
							,$"RX_OVERRIDE_FLAG"

							,$"MBU_CF_CD"
            ,$"F4.CLM_CD"
            ,$"F4.CLM_RPTG_1_CD"
            ,$"F4.CLM_RPTG_2_CD"
            ,$"F4.CLM_RPTG_3_CD"
            ,$"F4.EMP_NBR"
            ,$"F4.PLAN_GRP_ID"
            ,$"F4.CDHP_CTGRY_CD"
            ,$"F4.MBR_NTWK_ID"
            ,$"F4.PRMRY_CVRG_CD"
            ,$"F4.MEDCR_CD").distinct()

  CIIUtilities.writeDFtoS3(aDataSrcCfg,FRAME5,"frame5")

  val frame5_new =CIIUtilities.readDFfromS3(aDataSrcCfg,"frame5")

  frame5_new.createOrReplaceTempView("xwalk_stg")

  val mstr_sgmnt_elgbl= broadcast(getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(2))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer))) //ERSU

  mstr_sgmnt_elgbl.createOrReplaceTempView("mstr_sgmnt_elgbl")

  val WORK_GRP_SGMNTN_USED_tbl= broadcast(getSparkSession.table(s"${dbname}.WORK_GRP_SGMNTN_USED"))

  WORK_GRP_SGMNTN_USED_tbl.createOrReplaceTempView("WORK_GRP_SGMNTN_USED_tbl")

  val Final_Ersu_1= getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(3))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)) //ERSU
  val Final_Ersu_Final= Final_Ersu_1.withColumn("load_dtm", typedLit(current_timestamp).cast(TimestampType))
    .withColumn("load_log_key", lit(aDataSrcCfg.loadlogkey))

  val df_acct_sgmntnt_Mbr_Stg= castColumnTypes(Final_Ersu_Final, aSchemaDef)

  df_acct_sgmntnt_Mbr_Stg

 }
}

