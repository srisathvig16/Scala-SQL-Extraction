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

class WorkSgmntGlblDAO extends DataSFAccessObject  {

override def importData(aDataSrcCfg: DataSourceSFConfig, aSchemaDef: StructType): DataFrame = {
val sqlContext = getSparkSession.sqlContext
import sqlContext.implicits._
val env = aDataSrcCfg.env
val release = aDataSrcCfg.release
val layer = aDataSrcCfg.layer
//val dbname = s"${env}_pcaciirph_nogbd_${release}_${layer}"
val dbname_gbd = s"${env}_cii_discover"
    val dbname = s"${env}_cii_discover"
    val dbname_cdl = s"${env}_cii_discover"
    val dbname_adhoc = s"${env}_CII_ADHOC_cons"
var sqls = Array[String]()

aDataSrcCfg.queryMap.asScala foreach (sqlFileMap => {
sqls = sqlFileMap._2.split(",")
})

 ///val df_RLTNSHP = broadcast(getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(2))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)))

val FLTR = getSparkSession.table(s"${dbname}.wrk_rsstd_fltr_glbl")
		.withColumn("MAX_FUNDG_CF_CD", max($"FUNDG_CF_CD").over(Window.partitionBy($"MBR_KEY" ,$"MBRSHP_SOR_CD" ,$"RLTD_PRCHSR_ORG_NBR" ,$"BNFT_PKG_KEY" ,$"MBR_PROD_ENRLMNT_EFCTV_DT" ,
$"ELGBLTY_CLNDR_MNTH_END_DT" ,$"PKG_NBR" ,$"PROD_SOR_CD" ,$"PROD_ID" ,$"BNFT_PKG_ID",$"CII_RMM_FLG")  )  )
           .select($"PRCHSR_ORG_NBR",
                   $"RLTD_PRCHSR_ORG_NBR",
                   $"prod_id",
                   $"BNFT_PKG_ID",
                   $"PKG_NBR",
                   $"MBRSHP_SOR_CD",
                   $"RLTD_PRCHSR_ORG_TYPE_CD",
                   $"PROD_SOR_CD",
                   $"ELGBLTY_CLNDR_MNTH_END_DT",
                   $"MBR_KEY",
                   $"BNFT_PKG_KEY",
                   $"MBR_PROD_ENRLMNT_EFCTV_DT",
				   $"MAX_FUNDG_CF_CD"
                   ,$"FUNDG_CF_CD"
				   ,$"src_grp_nbr"
				   ,$"mdcl_mbr_cvrg_cnt"
				   ,$"vsn_mbr_cnt"
				   ,$"dntl_mbr_cnt"
				   ,$"mbr_prod_enrlmnt_trmntn_dt"
				   ,$"cntrct_type_cd"
				   ,$"othr_insrnc_type_cd"
				   ,$"gndr_cd"
				   ,$"mbr_mnth_cob_cd"
				   ,$"prcp_id"
				   ,$"prcp_ctgry_cd"
				   ,$"prcp_type_cd"
				   ,$"hc_id"
				   ,$"sbscrbr_id"
				   ,$"scrty_lvl_cd"
				   ,$"prod_ofrg_key"
				   ,$"mdcl_expsr_nbr"
				   ,$"phrmcy_mbr_expsr_nbr"
				   ,$"dntl_expsr_nbr"
				   ,$"vsn_expsr_nbr"
				   ,$"PHRMCY_MBR_CVRG_CNT"
				   ,$"MBU_CF_CD"
           ,$"CDHP_CTGRY_CD"
           ,$"cii_rmm_flg")
           
      
           
    
val DMGRPHC = getSparkSession.table(s"${dbname_gbd}.PRCHSR_ORG_DMGRPHC")
          .where(trim($"RCRD_STTS_CD")!=="DEL")
          .select(trim($"PRCHSR_ORG_CTGRY_TYPE_CD").as("PRCHSR_ORG_CTGRY_TYPE_CD"),
                  trim($"MBRSHP_SOR_CD").as("MBRSHP_SOR_CD"),
                  trim($"PRCHSR_ORG_NBR").as("PRCHSR_ORG_NBR"),
                   trim($"PRCHSR_ORG_TYPE_CD").as("PRCHSR_ORG_TYPE_CD"),
                  $"RCRD_STTS_CD" )

   print("+++++++++++++++++ DMGRPHC +++++++++++++++++++ \n")

val FRAME1= FLTR.as("FLTR").join(DMGRPHC.as("DMG"),trim($"FLTR.MBRSHP_SOR_CD")===$"DMG.MBRSHP_SOR_CD" &&
                 $"FLTR.RLTD_PRCHSR_ORG_NBR"===$"DMG.PRCHSR_ORG_NBR" &&
                 $"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"===$"DMG.PRCHSR_ORG_TYPE_CD","left_outer")
                .withColumn("SGMNTN_SUBGRP_ID",when($"FLTR.RLTD_PRCHSR_ORG_NBR".isin("","~01","~02","~03","~NA","~DV","UNK","NA")
                 || $"FLTR.RLTD_PRCHSR_ORG_NBR".isNull,concat($"FLTR.PRCHSR_ORG_NBR", lit("~UNK"))).otherwise($"FLTR.RLTD_PRCHSR_ORG_NBR"))
                 .withColumn("prod_id1",when($"FLTR.prod_id".isNull ,"UNK").otherwise($"FLTR.prod_id"))
                 .withColumn("SGMNTN_SOR_CD",when($"FLTR.MBRSHP_SOR_CD".isNull ,"UNK").otherwise(upper($"FLTR.MBRSHP_SOR_CD")))
                 .withColumn("GRP_STTS_CD",when($"DMG.PRCHSR_ORG_CTGRY_TYPE_CD".isin("","~01","~02","~03","~NA","~DV","UNK")
                 || $"DMG.PRCHSR_ORG_CTGRY_TYPE_CD".isNull ,"NA").otherwise($"DMG.PRCHSR_ORG_CTGRY_TYPE_CD"))
                 .withColumn("GRP_PLAN_CD", when($"FLTR.MBRSHP_SOR_CD".isin("823") ,substring($"FLTR.PROD_ID",1,3)).otherwise($"FLTR.PROD_ID"))
                 .select(
                          $"FLTR.PRCHSR_ORG_NBR"
                         ,$"FLTR.RLTD_PRCHSR_ORG_NBR"
                         ,$"FLTR.prod_id"
                         ,$"FLTR.BNFT_PKG_ID"
                         ,$"FLTR.PKG_NBR"
                         ,$"FLTR.MBRSHP_SOR_CD"
                         ,$"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"
                         ,$"FLTR.PROD_SOR_CD"
                         ,$"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"
                         ,$"FLTR.MBR_KEY"
                         ,$"FLTR.BNFT_PKG_KEY"
                         ,$"FLTR.MBR_PROD_ENRLMNT_EFCTV_DT"
                         ,$"DMG.PRCHSR_ORG_CTGRY_TYPE_CD"
                         ,$"DMG.PRCHSR_ORG_TYPE_CD"
                         ,$"DMG.RCRD_STTS_CD"
                         ,$"SGMNTN_SUBGRP_ID"
                         ,$"prod_id1"
                         ,$"SGMNTN_SOR_CD"
                         ,$"GRP_STTS_CD",$"MAX_FUNDG_CF_CD"
                         ,$"GRP_PLAN_CD"
						 ,$"FLTR.FUNDG_CF_CD"
						 ,$"FLTR.src_grp_nbr"
						 ,$"FLTR.mdcl_mbr_cvrg_cnt"
						 ,$"FLTR.vsn_mbr_cnt"
						 ,$"FLTR.dntl_mbr_cnt"
						 ,$"FLTR.mbr_prod_enrlmnt_trmntn_dt"
						 ,$"FLTR.cntrct_type_cd"
						 ,$"FLTR.othr_insrnc_type_cd"
						 ,$"FLTR.gndr_cd"
						 ,$"FLTR.mbr_mnth_cob_cd"
						 ,$"FLTR.prcp_id"
						 ,$"FLTR.prcp_ctgry_cd"
						 ,$"FLTR.prcp_type_cd"
						 ,$"FLTR.hc_id"
						 ,$"FLTR.sbscrbr_id"
						 ,$"FLTR.scrty_lvl_cd"
						 ,$"FLTR.prod_ofrg_key"
						 ,$"FLTR.mdcl_expsr_nbr"
						 ,$"FLTR.phrmcy_mbr_expsr_nbr"
						 ,$"FLTR.dntl_expsr_nbr"
						 ,$"FLTR.vsn_expsr_nbr"
						 ,$"FLTR.PHRMCY_MBR_CVRG_CNT"
						 ,$"FLTR.MBU_CF_CD"
             ,$"CDHP_CTGRY_CD"
             ,$"FLTR.CII_RMM_FLG")
     
      
      
  // segmentation add derivation for CDHP_CTGRY_CD & PLAN_GRP_ID
  // segmentation also we can add derivation for NTWK_ID
//read cii_wrk_product_table.sql to derive CDHP_CTGRY_CD & PLAN_GRP_ID
  //val df_interm_product = broadcast(getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(5))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)))

  //segmentation update later remove hard coding
val WRK_PROD = getSparkSession.table(s"${dbname}.work_prod_data")
  val WRK_ECR_SCD_PROD = getSparkSession.table(s"${dbname}.ECR_SCD_PROD")
  //val PROD_OFRG_NTWK = getSparkSession.table(s"${dbname}.PROD_OFRG_NTWK")
    //.groupBy($"PROD_OFRG_KEY",$"PROD_OFRG_NTWK_EFCTV_DT", $"PROD_OFRG_NTWK_TRMNTN_DT").agg(max($"NTWK_ID").as("MAX_NTWK_ID"))
      //.select($"PROD_OFRG_KEY",$"PROD_OFRG_NTWK_EFCTV_DT",$"PROD_OFRG_NTWK_TRMNTN_DT",$"MAX_NTWK_ID".as("NTWK_ID"))
      
val PROD_OFRG_NTWK=getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(0))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)) //prod_ofrg_ntwk.sql



  val FRAME1_A  = FRAME1.as("FLTR").join(WRK_PROD.as("INTRM_PRODCT"),trim($"FLTR.GRP_PLAN_CD")===$"INTRM_PRODCT.GRP_PLAN_CD" &&
                           $"FLTR.MBRSHP_SOR_CD"===$"INTRM_PRODCT.MBRSHP_SOR_CD" , "left_outer")
                      .join(WRK_ECR_SCD_PROD.as("WRK_ECR_SCD_PROD"),trim($"FLTR.PROD_ID")===$"WRK_ECR_SCD_PROD.PROD_ID" &&
                           $"FLTR.PROD_SOR_CD"===$"WRK_ECR_SCD_PROD.PROD_SOR_CD" , "left_outer")
                      .join(PROD_OFRG_NTWK.as("PROD_OFRG_NTWK"),trim($"FLTR.PROD_OFRG_KEY")===$"PROD_OFRG_NTWK.PROD_OFRG_KEY" &&
                           $"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"===$"PROD_OFRG_NTWK.NTWK_DT" , "left_outer")

    .withColumn("PLAN_GRP_ID",when($"INTRM_PRODCT.GRP_PLAN_CD".isNotNull, $"INTRM_PRODCT.RPTG_PLAN_GRP_ID")
                              .when($"WRK_ECR_SCD_PROD.PROD_ID".isNotNull && $"WRK_ECR_SCD_PROD.PROD_SOR_CD".isNotNull
                                    && ($"WRK_ECR_SCD_PROD.PLAN_GRP_ID".isin("","~01","~02","~03","~NA","~DV","UNK","NA")
                                    || $"WRK_ECR_SCD_PROD.PLAN_GRP_ID".isNull),"NA")
                              .when($"WRK_ECR_SCD_PROD.PROD_ID".isNotNull && $"WRK_ECR_SCD_PROD.PROD_SOR_CD".isNotNull
                                      ,upper(trim(regexp_replace($"PLAN_GRP_ID","\\^", "\\s"))))
                              .otherwise("NA"))

    .withColumn("CDHP_CTGRY_CD1",when($"INTRM_PRODCT.GRP_PLAN_CD".isNotNull, upper(trim(regexp_replace($"INTRM_PRODCT.RPTG_CDHP_CTGRY_CD","\\^", "\\s"))))
                                .when($"FLTR.CDHP_CTGRY_CD".isin("","~01","~02","~03","~NA","~DV","UNK","NA")
                                  || $"FLTR.CDHP_CTGRY_CD".isNull,"NA")
                                .otherwise(upper(trim(regexp_replace($"FLTR.CDHP_CTGRY_CD","\\^", "\\s")))))

     .withColumn("MBR_NTWK_ID",when($"PROD_OFRG_NTWK.NTWK_ID".isin("","~01","~02","~03","~NA","~DV","UNK","NA")
                                  || $"PROD_OFRG_NTWK.NTWK_ID".isNull,"NA")
                               .otherwise(upper(trim(regexp_replace($"PROD_OFRG_NTWK.NTWK_ID","\\^", "\\s")))))

         .select(
           $"FLTR.PRCHSR_ORG_NBR"
           ,$"FLTR.RLTD_PRCHSR_ORG_NBR"
           ,$"FLTR.prod_id"
           ,$"FLTR.BNFT_PKG_ID"
           ,$"FLTR.PKG_NBR"
           ,$"FLTR.MBRSHP_SOR_CD"
           ,$"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"
           ,$"FLTR.PROD_SOR_CD"
           ,$"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"
           ,$"FLTR.MBR_KEY"
           ,$"FLTR.BNFT_PKG_KEY"
           ,$"FLTR.MBR_PROD_ENRLMNT_EFCTV_DT"
           ,$"PRCHSR_ORG_CTGRY_TYPE_CD"
           ,$"PRCHSR_ORG_TYPE_CD"
           ,$"RCRD_STTS_CD"
           ,$"SGMNTN_SUBGRP_ID"
           ,$"prod_id1"
           ,$"SGMNTN_SOR_CD"
           ,$"GRP_STTS_CD"
           ,$"MAX_FUNDG_CF_CD"
           ,$"FLTR.GRP_PLAN_CD"
           ,$"FLTR.FUNDG_CF_CD"
           ,$"FLTR.src_grp_nbr"
           ,$"FLTR.mdcl_mbr_cvrg_cnt"
           ,$"FLTR.vsn_mbr_cnt"
           ,$"FLTR.dntl_mbr_cnt"
           ,$"FLTR.mbr_prod_enrlmnt_trmntn_dt"
           ,$"FLTR.cntrct_type_cd"
           ,$"FLTR.othr_insrnc_type_cd"
           ,$"FLTR.gndr_cd"
           ,$"FLTR.mbr_mnth_cob_cd"
           ,$"FLTR.prcp_id"
           ,$"FLTR.prcp_ctgry_cd"
           ,$"FLTR.prcp_type_cd"
           ,$"FLTR.hc_id"
           ,$"FLTR.sbscrbr_id"
           ,$"FLTR.scrty_lvl_cd"
           ,$"FLTR.prod_ofrg_key"
           ,$"FLTR.mdcl_expsr_nbr"
           ,$"FLTR.phrmcy_mbr_expsr_nbr"
           ,$"FLTR.dntl_expsr_nbr"
           ,$"FLTR.vsn_expsr_nbr"
           ,$"FLTR.PHRMCY_MBR_CVRG_CNT"
           ,$"FLTR.MBU_CF_CD"
         ,$"PLAN_GRP_ID"
         ,$"CDHP_CTGRY_CD1".as("CDHP_CTGRY_CD")
         ,$"MBR_NTWK_ID"
         ,$"cii_rmm_flg")




val PLAN_REF= getSparkSession.table(s"${dbname}.CII_DDIM_PLAN_REF")
val  df_prd=broadcast(getSparkSession.table(s"${dbname_gbd}.PROD").filter($"RCRD_STTS_CD".notEqual("DEL")
                   && !$"PROD_SOR_CD".isin("1037", "1026", "1028", "1029", "1030", "1034", "1060","1090", "1091", "1092",
                   "1093", "1094", "1095", "1096", "1097", "1098", "1099")) )

val FRAME2= FRAME1_A.as("F1").join(broadcast(PLAN_REF).as("PLRF"),$"F1.MBRSHP_SOR_CD"=== $"PLRF.SOR_CD","left_outer")
           .join(WRK_PROD.as("WPRD"),$"F1.GRP_PLAN_CD"=== $"WPRD.GRP_PLAN_CD" && $"F1.MBRSHP_SOR_CD"=== $"WPRD.MBRSHP_SOR_CD","left_outer")
		   .join(df_prd.as("prd"),$"F1.PROD_SOR_CD"=== $"prd.PROD_SOR_CD" && $"F1.PROD_ID" === $"prd.PROD_ID" ,"left_outer")
            .withColumn("HLTH_PROD_SRVC_TYPE_CD1", 
            when($"F1.DNTL_MBR_CNT"!==0 ,"DEN")
            .when($"F1.VSN_MBR_CNT"!==0,"VIS")
			.when($"WPRD.HLTH_PROD_SRVC_TYPE_CD".isin("","~01","~02","~03","~NA","~DV","UNK") || $"WPRD.HLTH_PROD_SRVC_TYPE_CD".isNull ,coalesce($"prd.HLTH_PROD_SRVC_TYPE_CD",lit("NA")))
			.otherwise(upper($"WPRD.HLTH_PROD_SRVC_TYPE_CD")))
.withColumn("ORIG_HLTH_PROD_SRVC_TYPE_CD", when($"prd.HLTH_PROD_SRVC_TYPE_CD".isin("","~01","~02","~03","~NA","~DV","UNK") || $"WPRD.HLTH_PROD_SRVC_TYPE_CD".isNull 
,coalesce($"prd.HLTH_PROD_SRVC_TYPE_CD",lit("NA")))
            .otherwise(upper($"WPRD.HLTH_PROD_SRVC_TYPE_CD")))
            .withColumn("PLAN_ID1",
when($"PLRF.SOR_CD".isNotNull && $"PLRF.BNFT_PKG_ID_IND".isNotNull && $"PLRF.PKG_NUM_IND".isNotNull , $"F1.BNFT_PKG_ID")
.when($"PLRF.SOR_CD".isNotNull && $"PLRF.BNFT_PKG_ID_IND".isNotNull && $"PLRF.PKG_NUM_IND".isNull , $"F1.BNFT_PKG_ID")
.when($"PLRF.SOR_CD".isNotNull && $"PLRF.BNFT_PKG_ID_IND".isNull && $"PLRF.PKG_NUM_IND".isNotNull,$"F1.PKG_NBR")
.when($"PLRF.SOR_CD".isNotNull && $"PLRF.BNFT_PKG_ID_IND".isNull && $"PLRF.PKG_NUM_IND".isNull,$"F1.BNFT_PKG_ID").otherwise($"F1.BNFT_PKG_ID")

                      )
.withColumn("RX_OVERRIDE_FLAG", when ($"cii_rmm_flg"==="Y" 
														&& !$"prd.PROD_SOR_CD".isNull &&  $"F1.PRCP_TYPE_CD"=== "06" 
														&& $"F1.MBRSHP_SOR_CD".isin("886","868")
                             &&  (($"prd.CDHP_ADMNSTRN_CD" === "UNK" && !$"F1.PROD_ID".isin("PPOIHAPMCMHS"))
                             || ($"prd.CDHP_ADMNSTRN_CD" ==="LUM" && $"F1.PROD_ID".isin("PPONHSKMPVHA","PPONSMKMPVHS"))
                             || ($"prd.CDHP_ADMNSTRN_CD" !=="LUM")) , "Y")
                       .when($"cii_rmm_flg"==="Y" && !$"prd.PROD_SOR_CD".isNull && $"F1.PRCP_TYPE_CD"=== "06" 
                       && $"F1.MBRSHP_SOR_CD".isin("867","878") &&
                              !$"F1.PROD_ID".like("HM%") &&  !$"F1.PROD_ID".like("RX%") &&
                   !$"F1.PROD_ID".isin("PPFS","PPFT","PPFU","PPFV","PPH3","PPH4","PPH5","PPH6","PPHA","PPHB","PPHD","PPHE","PPHP","PPHR","PPHS","PPHT",
                             "PPMC","PPMI","PPMM","PPMS", "PPR3","PPR4","PPR5","PPR6","PPRA","PPRB","PPRD","PPRE","PPXE","PPXF","PPXG","PPXH"),"Y"
                         ).otherwise("N"))
                      .select(
                       $"F1.PRCHSR_ORG_NBR"
                      ,$"F1.RLTD_PRCHSR_ORG_NBR"
                      ,$"F1.prod_id"
                      ,$"F1.BNFT_PKG_ID"
                      ,$"F1.PKG_NBR"
                      ,$"F1.MBRSHP_SOR_CD"
                      ,$"F1.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"F1.PROD_SOR_CD"
                      ,$"F1.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"F1.MBR_KEY"
                      ,$"F1.BNFT_PKG_KEY"
                      ,$"F1.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"F1.PRCHSR_ORG_CTGRY_TYPE_CD"
                      ,$"F1.PRCHSR_ORG_TYPE_CD"
                      ,$"F1.RCRD_STTS_CD"
                      ,$"F1.SGMNTN_SUBGRP_ID"
                      ,$"F1.prod_id1"
                      ,$"F1.SGMNTN_SOR_CD"
                      ,$"F1.GRP_STTS_CD"
                      ,when($"HLTH_PROD_SRVC_TYPE_CD1".isin("","~01","~02","~03","~NA","~DV","UNK"),"NA").otherwise($"HLTH_PROD_SRVC_TYPE_CD1").as("HLTH_PROD_SRVC_TYPE_CD")
                      ,$"PLAN_ID1",$"MAX_FUNDG_CF_CD"
					  ,$"F1.src_grp_nbr"
					  ,$"F1.FUNDG_CF_CD"
				   ,$"F1.mdcl_mbr_cvrg_cnt"
				   ,$"F1.vsn_mbr_cnt"
				   ,$"F1.dntl_mbr_cnt"
				   ,$"F1.mbr_prod_enrlmnt_trmntn_dt"
				   ,$"F1.cntrct_type_cd"
				   ,$"F1.othr_insrnc_type_cd"
				   ,$"F1.gndr_cd"
				   ,$"F1.mbr_mnth_cob_cd"
				   ,$"F1.prcp_id"
				   ,$"F1.prcp_ctgry_cd"
				   ,$"F1.prcp_type_cd"
				   ,$"F1.hc_id"
				   ,$"F1.sbscrbr_id"
				   ,$"F1.scrty_lvl_cd"
				   ,$"F1.prod_ofrg_key"
				   ,$"F1.mdcl_expsr_nbr"
				   ,$"F1.phrmcy_mbr_expsr_nbr"
				   ,$"F1.dntl_expsr_nbr"
				   ,$"F1.vsn_expsr_nbr"
				   ,$"F1.PHRMCY_MBR_CVRG_CNT"
				   ,$"RX_OVERRIDE_FLAG"
				   ,when($"ORIG_HLTH_PROD_SRVC_TYPE_CD".isin("","~01","~02","~03","~NA","~DV","UNK"),"NA").otherwise($"ORIG_HLTH_PROD_SRVC_TYPE_CD").as("ORIG_HLTH_PROD_SRVC_TYPE_CD")
				   ,$"F1.MBU_CF_CD"
           ,$"F1.PLAN_GRP_ID"
           ,$"F1.CDHP_CTGRY_CD"
           ,$"F1.MBR_NTWK_ID"
           ,$"cii_rmm_flg")
	   

	     


val df_bot_FULLY_INSRD =getSparkSession.table(s"${dbname}.BOT_CII_FULLY_INSRD")
//segmentation adding the source columns in 0th sql from emplymnt. Just need to select here  in df_EMPLR_GRP_DEPT

val df_emp=getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(1))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)) //cii_dept.sql

val df_EMPLR_GRP_DEPT= FRAME2.as("FLTR").join(df_emp.as("DPT"),
$"FLTR.MBR_KEY" === $"DPT.MBR_KEY" && $"FLTR.MBRSHP_SOR_CD" === $"DPT.MBRSHP_SOR_CD"
&& $"FLTR.RLTD_PRCHSR_ORG_NBR" === $"DPT.PRCHSR_ORG_NBR"

 && $"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"=== $"DPT.ELGBLTY_CLNDR_MNTH_END_DT","left_outer")
.withColumn("EMPLR_DEPT_NBR", when ($"DPT.EMPLR_GRP_DEPT_NBR".isin("","~01","~02","~03","~NA","~DV","UNK") || $"DPT.EMPLR_GRP_DEPT_NBR".isNull,"NA").otherwise(upper($"DPT.EMPLR_GRP_DEPT_NBR")))
.select(
    $"FLTR.PRCHSR_ORG_NBR"
                      ,$"FLTR.RLTD_PRCHSR_ORG_NBR"
                      ,$"FLTR.prod_id"
                      ,$"FLTR.BNFT_PKG_ID"
                      ,$"FLTR.PKG_NBR"
                      ,$"FLTR.MBRSHP_SOR_CD"
                      ,$"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.PROD_SOR_CD"
                      ,$"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"FLTR.MBR_KEY"
                      ,$"FLTR.BNFT_PKG_KEY"
                      ,$"FLTR.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"FLTR.PRCHSR_ORG_CTGRY_TYPE_CD"
                      ,$"FLTR.PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.RCRD_STTS_CD"
                      ,$"FLTR.SGMNTN_SUBGRP_ID"
                      ,$"FLTR.prod_id1"
                      ,$"FLTR.SGMNTN_SOR_CD"
                      ,$"FLTR.GRP_STTS_CD"
                      ,$"FLTR.HLTH_PROD_SRVC_TYPE_CD"
                      ,$"FLTR.PLAN_ID1"
                      ,trim($"EMPLR_DEPT_NBR").as("EMPLR_DEPT_NBR")
                      ,$"FLTR.MAX_FUNDG_CF_CD"

    ,$"FLTR.FUNDG_CF_CD"
					  ,$"FLTR.src_grp_nbr"
				   ,$"FLTR.mdcl_mbr_cvrg_cnt"
				   ,$"FLTR.vsn_mbr_cnt"
				   ,$"FLTR.dntl_mbr_cnt"
				   ,$"FLTR.mbr_prod_enrlmnt_trmntn_dt"
				   ,$"FLTR.cntrct_type_cd"
				   ,$"FLTR.othr_insrnc_type_cd"
				   ,$"FLTR.gndr_cd"
				   ,$"FLTR.mbr_mnth_cob_cd"
				   ,$"FLTR.prcp_id"
				   ,$"FLTR.prcp_ctgry_cd"
				   ,$"FLTR.prcp_type_cd"
				   ,$"FLTR.hc_id"
				   ,$"FLTR.sbscrbr_id"
				   ,$"FLTR.scrty_lvl_cd"
				   ,$"FLTR.prod_ofrg_key"
				   ,$"FLTR.mdcl_expsr_nbr"
				   ,$"FLTR.phrmcy_mbr_expsr_nbr"
				   ,$"FLTR.dntl_expsr_nbr"
				   ,$"FLTR.vsn_expsr_nbr"
    ,$"FLTR.PHRMCY_MBR_CVRG_CNT"
    ,$"FLTR.RX_OVERRIDE_FLAG"
	,$"FLTR.ORIG_HLTH_PROD_SRVC_TYPE_CD"
    ,$"FLTR.MBU_CF_CD"
  ,when($"DPT.CLM_RPTG_CTGRY_TXT".isin("","~01","~02","~03","~NA","~DV","UNK") || $"DPT.CLM_RPTG_CTGRY_TXT".isNull,"NA").otherwise(upper(trim(regexp_replace($"DPT.CLM_RPTG_CTGRY_TXT","\\^", "\\s")))).as("CLM_CD")
  ,when($"DPT.CLM_RPTG_1_TXT".isin("","~01","~02","~03","~NA","~DV","UNK") || $"DPT.CLM_RPTG_1_TXT".isNull,"NA").otherwise(upper(trim(regexp_replace($"DPT.CLM_RPTG_1_TXT","\\^", "\\s")))).as("CLM_RPTG_1_CD")
  ,when($"DPT.CLM_RPTG_2_TXT".isin("","~01","~02","~03","~NA","~DV","UNK") || $"DPT.CLM_RPTG_2_TXT".isNull,"NA").otherwise(upper(trim(regexp_replace($"DPT.CLM_RPTG_2_TXT","\\^", "\\s")))).as("CLM_RPTG_2_CD")
  ,when($"DPT.CLM_RPTG_3_TXT".isin("","~01","~02","~03","~NA","~DV","UNK") || $"DPT.CLM_RPTG_3_TXT".isNull,"NA").otherwise(upper(trim(regexp_replace($"DPT.CLM_RPTG_3_TXT","\\^", "\\s")))).as("CLM_RPTG_3_CD")
  ,when($"DPT.EMP_NBR".isin("","~EZ","~NZ","~ZZ","~UD","~AL","UNK") || $"DPT.EMP_NBR".isNull,"NA").otherwise(upper(trim($"DPT.EMP_NBR"))).as("EMP_NBR")
  //  ,when($"DPT.EMP_NBR".isin("","~01","~02","~03","~NA","~DV","UNK") || $"DPT.EMP_NBR".isNull,"NA").otherwise(upper(trim(regexp_replace($"DPT.EMP_NBR","\\^", "\\s")))).as("EMP_NBR")
  ,$"FLTR.PLAN_GRP_ID"
  ,$"FLTR.CDHP_CTGRY_CD"
  ,$"FLTR.MBR_NTWK_ID"
  ,$"FLTR.CII_RMM_FLG" ) 
  

 


val df_rlshp=getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(2))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)) //cii_wrk_rlsph.sql
val df_rlshp2=getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(3))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)) //cii_wrk_rlsph2.sql



val df_rlshp12= df_EMPLR_GRP_DEPT.as("FLTR").join(df_rlshp.as("RLSHP"),
      $"FLTR.MBRSHP_SOR_CD" === $"RLSHP.MBRSHP_SOR_CD" && $"FLTR.PRCHSR_ORG_NBR" === $"RLSHP.RLTD_PRCHSR_ORG_NBR"
      && $"FLTR.ELGBLTY_CLNDR_MNTH_END_DT" === $"RLSHP.ELGBLTY_CLNDR_MNTH_END_DT"
  ,"left_outer")
.withColumn("SGMNTN_ASSN_ID", when( !$"RLSHP.RLTD_PRCHSR_ORG_NBR".isNull && trim($"RLSHP.PRCHSR_ORG_TYPE_CD") === lit("06")
   ,upper(trim(regexp_replace($"RLSHP.PRCHSR_ORG_NBR","\\^", "\\s"))))
   .when($"RLSHP.RLTD_PRCHSR_ORG_NBR".isNull && trim($"RLSHP.PRCHSR_ORG_TYPE_CD")=== lit("06"),lit("UNK"))
   .otherwise(lit("NA"))   )

  .select(
    $"FLTR.PRCHSR_ORG_NBR"
                      ,$"FLTR.RLTD_PRCHSR_ORG_NBR"
                      ,$"FLTR.prod_id"
                      ,$"FLTR.BNFT_PKG_ID"
                      ,$"FLTR.PKG_NBR"
                      ,$"FLTR.MBRSHP_SOR_CD"
                      ,$"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.PROD_SOR_CD"
                      ,$"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"FLTR.MBR_KEY"
                      ,$"FLTR.BNFT_PKG_KEY"
                      ,$"FLTR.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"FLTR.PRCHSR_ORG_CTGRY_TYPE_CD"
                      ,$"FLTR.PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.RCRD_STTS_CD"
                      ,$"FLTR.SGMNTN_SUBGRP_ID"
                      ,$"FLTR.prod_id1"
                      ,$"FLTR.SGMNTN_SOR_CD"
                      ,$"FLTR.GRP_STTS_CD"
                      ,$"FLTR.HLTH_PROD_SRVC_TYPE_CD"
                      ,$"FLTR.PLAN_ID1"
                      ,$"FLTR.EMPLR_DEPT_NBR"
                      ,$"FLTR.MAX_FUNDG_CF_CD"
                      ,$"SGMNTN_ASSN_ID"
					  ,$"FLTR.FUNDG_CF_CD"
					 ,$"FLTR.src_grp_nbr"
				     ,$"FLTR.mdcl_mbr_cvrg_cnt"
				     ,$"FLTR.vsn_mbr_cnt"
				     ,$"FLTR.dntl_mbr_cnt"
				     ,$"FLTR.mbr_prod_enrlmnt_trmntn_dt"
				     ,$"FLTR.cntrct_type_cd"
				     ,$"FLTR.othr_insrnc_type_cd"
				     ,$"FLTR.gndr_cd"
				     ,$"FLTR.mbr_mnth_cob_cd"
				     ,$"FLTR.prcp_id"
				     ,$"FLTR.prcp_ctgry_cd"
				     ,$"FLTR.prcp_type_cd"
				     ,$"FLTR.hc_id"
				     ,$"FLTR.sbscrbr_id"
				     ,$"FLTR.scrty_lvl_cd"
				     ,$"FLTR.prod_ofrg_key"
				     ,$"FLTR.mdcl_expsr_nbr"
				     ,$"FLTR.phrmcy_mbr_expsr_nbr"
				     ,$"FLTR.dntl_expsr_nbr"
				     ,$"FLTR.vsn_expsr_nbr"
				     ,$"FLTR.PHRMCY_MBR_CVRG_CNT"
				     ,$"FLTR.RX_OVERRIDE_FLAG"
					 ,$"FLTR.ORIG_HLTH_PROD_SRVC_TYPE_CD"
					 ,$"FLTR.MBU_CF_CD"
    ,$"FLTR.CLM_CD"
    ,$"FLTR.CLM_RPTG_1_CD"
    ,$"FLTR.CLM_RPTG_2_CD"
    ,$"FLTR.CLM_RPTG_3_CD"
    ,$"FLTR.EMP_NBR"
    ,$"FLTR.PLAN_GRP_ID"
    ,$"FLTR.CDHP_CTGRY_CD"
    ,$"FLTR.MBR_NTWK_ID"
    ,$"FLTR.CII_RMM_FLG"
    )
 
   
  

val df_rlshp13= df_rlshp12.as("FLTR")
  .join(df_rlshp2.as("RLSHP2"),
      $"FLTR.MBRSHP_SOR_CD" === $"RLSHP2.MBRSHP_SOR_CD" && $"FLTR.PRCHSR_ORG_NBR" === $"RLSHP2.RLTD_PRCHSR_ORG_NBR"
      && $"FLTR.ELGBLTY_CLNDR_MNTH_END_DT" === $"RLSHP2.ELGBLTY_CLNDR_MNTH_END_DT"
  ,"left_outer")
 .withColumn("SGMNTN_CLNT_ID", when( !$"RLSHP2.RLTD_PRCHSR_ORG_NBR".isNull && trim($"RLSHP2.PRCHSR_ORG_TYPE_CD") === lit("02")
   ,upper(trim(regexp_replace($"RLSHP2.PRCHSR_ORG_NBR","\\^", "\\s"))))
   .when($"RLSHP2.RLTD_PRCHSR_ORG_NBR".isNull && trim($"RLSHP2.PRCHSR_ORG_TYPE_CD")=== lit("02"),lit("UNK"))
   .otherwise(lit("NA"))   )
  .select(
    $"FLTR.PRCHSR_ORG_NBR"
                      ,$"FLTR.RLTD_PRCHSR_ORG_NBR"
                      ,$"FLTR.prod_id"
                      ,$"FLTR.BNFT_PKG_ID"
                      ,$"FLTR.PKG_NBR"
                      ,$"FLTR.MBRSHP_SOR_CD"
                      ,$"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.PROD_SOR_CD"
                      ,$"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"FLTR.MBR_KEY"
                      ,$"FLTR.BNFT_PKG_KEY"
                      ,$"FLTR.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"FLTR.PRCHSR_ORG_CTGRY_TYPE_CD"
                      ,$"FLTR.PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.RCRD_STTS_CD"
                      ,$"FLTR.SGMNTN_SUBGRP_ID"
                      ,$"FLTR.prod_id1"
                      ,$"FLTR.SGMNTN_SOR_CD"
                      ,$"FLTR.GRP_STTS_CD"
                      ,$"FLTR.HLTH_PROD_SRVC_TYPE_CD"
                      ,$"FLTR.PLAN_ID1"
                      ,$"FLTR.EMPLR_DEPT_NBR"
                      ,$"FLTR.MAX_FUNDG_CF_CD"
                      ,$"SGMNTN_ASSN_ID"
                      ,$"SGMNTN_CLNT_ID"
,$"FLTR.FUNDG_CF_CD"
					 ,$"FLTR.src_grp_nbr"
				     ,$"FLTR.mdcl_mbr_cvrg_cnt"
				     ,$"FLTR.vsn_mbr_cnt"
				     ,$"FLTR.dntl_mbr_cnt"
				     ,$"FLTR.mbr_prod_enrlmnt_trmntn_dt"
				     ,$"FLTR.cntrct_type_cd"
				     ,$"FLTR.othr_insrnc_type_cd"
				     ,$"FLTR.gndr_cd"
				     ,$"FLTR.mbr_mnth_cob_cd"
				     ,$"FLTR.prcp_id"
				     ,$"FLTR.prcp_ctgry_cd"
				     ,$"FLTR.prcp_type_cd"
				     ,$"FLTR.hc_id"
				     ,$"FLTR.sbscrbr_id"
				     ,$"FLTR.scrty_lvl_cd"
				     ,$"FLTR.prod_ofrg_key"
				     ,$"FLTR.mdcl_expsr_nbr"
				     ,$"FLTR.phrmcy_mbr_expsr_nbr"
				     ,$"FLTR.dntl_expsr_nbr"
				     ,$"FLTR.vsn_expsr_nbr"
				     ,$"FLTR.PHRMCY_MBR_CVRG_CNT"
				     ,$"FLTR.RX_OVERRIDE_FLAG"
					 ,$"FLTR.ORIG_HLTH_PROD_SRVC_TYPE_CD"
					 ,$"FLTR.MBU_CF_CD"
    ,$"FLTR.CLM_CD"
    ,$"FLTR.CLM_RPTG_1_CD"
    ,$"FLTR.CLM_RPTG_2_CD"
    ,$"FLTR.CLM_RPTG_3_CD"
    ,$"FLTR.EMP_NBR"
    ,$"FLTR.PLAN_GRP_ID"
    ,$"FLTR.CDHP_CTGRY_CD"
    ,$"FLTR.MBR_NTWK_ID"
    ,$"FLTR.CII_RMM_FLG"
    )
    
    
 

val df_wrk_wth_insrd= df_rlshp13.as("FLT1").join(broadcast(df_bot_FULLY_INSRD).as("bot_FULLY_INSRD"),$"bot_FULLY_INSRD.FUNDG_CF_CD" === $"FLT1.MAX_FUNDG_CF_CD"
        ,"left_outer")
       .withColumn("FULLY_INSRD_CD", when (!$"bot_FULLY_INSRD.FUNDG_CF_CD".isNull && !$"bot_FULLY_INSRD.RPTG_FUNDG_RLUP_CD".isNull ,upper($"bot_FULLY_INSRD.RPTG_FUNDG_RLUP_CD")).otherwise(lit("I")))
       .select(
    upper($"FLT1.PRCHSR_ORG_NBR").as("SGMNTN_GRP_ID")
                      ,$"FLT1.RLTD_PRCHSR_ORG_NBR"
                      ,$"FLT1.prod_id"
					  ,$"FLT1.BNFT_PKG_ID"
                      ,when($"FLT1.PLAN_ID1".isNull, "UNK").otherwise(upper($"FLT1.PLAN_ID1")).as("PLAN_ID")
                      ,$"FLT1.PKG_NBR"
                      ,$"FLT1.MBRSHP_SOR_CD"
                      ,$"FLT1.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"FLT1.PROD_SOR_CD"
                      ,$"FLT1.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"FLT1.MBR_KEY"
                      ,$"FLT1.BNFT_PKG_KEY"
                      ,$"FLT1.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"FLT1.PRCHSR_ORG_CTGRY_TYPE_CD"
                      ,$"FLT1.PRCHSR_ORG_TYPE_CD"
                      ,$"FLT1.RCRD_STTS_CD"
                      ,$"FLT1.SGMNTN_SUBGRP_ID"
                      ,$"FLT1.prod_id1"//.as("prod_id")
                      ,$"FLT1.SGMNTN_SOR_CD"
                      ,$"FLT1.GRP_STTS_CD"
                      ,$"FLT1.HLTH_PROD_SRVC_TYPE_CD"
                      ,$"FLT1.EMPLR_DEPT_NBR"//when ($"FLT1.EMPLR_DEPT_NBR" === "","UNK").otherwise($"FLT1.EMPLR_DEPT_NBR").as("EMPLR_DEPT_NBR")
                      ,$"FLT1.MAX_FUNDG_CF_CD"
                      ,$"FLT1.SGMNTN_ASSN_ID"
                      ,$"FLT1.SGMNTN_CLNT_ID"
                      ,$"FULLY_INSRD_CD"
					  ,$"FLT1.FUNDG_CF_CD"
					  ,$"FLT1.src_grp_nbr"      //
					  ,$"FLT1.mdcl_mbr_cvrg_cnt"
					  ,$"FLT1.vsn_mbr_cnt"
					  ,$"FLT1.dntl_mbr_cnt"
					  ,$"FLT1.mbr_prod_enrlmnt_trmntn_dt"
					  ,$"FLT1.cntrct_type_cd"
					  ,$"FLT1.othr_insrnc_type_cd"
					  ,$"FLT1.gndr_cd"
					  ,$"FLT1.mbr_mnth_cob_cd"
					  ,$"FLT1.prcp_id"
					  ,$"FLT1.prcp_ctgry_cd"
					  ,$"FLT1.prcp_type_cd"
					  ,$"FLT1.hc_id"
					  ,$"FLT1.sbscrbr_id"
					  ,$"FLT1.scrty_lvl_cd"
					  ,$"FLT1.prod_ofrg_key"
					  ,$"FLT1.mdcl_expsr_nbr"
					  ,$"FLT1.phrmcy_mbr_expsr_nbr"
					  ,$"FLT1.dntl_expsr_nbr"
					  ,$"FLT1.vsn_expsr_nbr"
					  ,$"FLT1.PHRMCY_MBR_CVRG_CNT"
					  ,$"FLT1.RX_OVERRIDE_FLAG"
					  ,$"FLT1.ORIG_HLTH_PROD_SRVC_TYPE_CD"
					  ,$"FLT1.MBU_CF_CD"
         ,$"FLT1.CLM_CD"
         ,$"FLT1.CLM_RPTG_1_CD"
         ,$"FLT1.CLM_RPTG_2_CD"
         ,$"FLT1.CLM_RPTG_3_CD"
         ,$"FLT1.EMP_NBR"
         ,$"FLT1.PLAN_GRP_ID"
         ,$"FLT1.CDHP_CTGRY_CD"
         ,$"FLT1.MBR_NTWK_ID"
         ,$"FLT1.CII_RMM_FLG"
    )
    
    
    
   
  //to get MBR_COB data for segmentation changes
  val df_5_yr_data = broadcast(getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(4))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)))
  df_5_yr_data.createOrReplaceTempView("Year_Month_Tbl_5yr")

  val df_mbr_cob = getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(5))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer))

  CIIUtilities.writeDFtoS3(aDataSrcCfg,df_mbr_cob,"df_mbr_cob")

  val tbl_mbr_cob =CIIUtilities.readDFfromS3(aDataSrcCfg,"df_mbr_cob")

  val df_wrk= df_wrk_wth_insrd.as("FLT1").join(tbl_mbr_cob.as("cob_data"),$"FLT1.MBR_KEY" === $"cob_data.MBR_KEY"
    && $"FLT1.ELGBLTY_CLNDR_MNTH_END_DT" === $"cob_data.LAST_DT_OF_THE_MNTH_DT" , "left_outer")
    .withColumn("PRMRY_CVRG_CD",when($"FLT1.MBR_MNTH_COB_CD".isin("","~01","~02","~03","~NA","~DV","UNK","NA")
      || $"FLT1.MBR_MNTH_COB_CD".isNull ||  !$"FLT1.MBR_MNTH_COB_CD".isin("P","PA") || $"OPL_ORDR_CD"===lit(1),"N")
         .otherwise("Y"))
    .withColumn("MEDCR_CD",when($"cob_data.OTHR_INSRNC_TYPE_CD".isin("MDCR","MEDCR")
      && $"OPL_ORDR_CD"===lit(1),"Y")
      .otherwise("N"))
    .select(
       $"FLT1.SGMNTN_GRP_ID"
      ,$"FLT1.RLTD_PRCHSR_ORG_NBR"
      ,$"FLT1.prod_id"
      ,$"FLT1.BNFT_PKG_ID"
      ,$"FLT1.PLAN_ID"
      ,$"FLT1.PKG_NBR"
      ,$"FLT1.MBRSHP_SOR_CD"
      ,$"FLT1.RLTD_PRCHSR_ORG_TYPE_CD"
      ,$"FLT1.PROD_SOR_CD"
      ,$"FLT1.ELGBLTY_CLNDR_MNTH_END_DT"
      ,$"FLT1.MBR_KEY"
      ,$"FLT1.BNFT_PKG_KEY"
      ,$"FLT1.MBR_PROD_ENRLMNT_EFCTV_DT"
      ,$"FLT1.SGMNTN_SUBGRP_ID"
      ,$"FLT1.prod_id1"
      ,$"FLT1.SGMNTN_SOR_CD"
      ,$"FLT1.GRP_STTS_CD"
      ,$"FLT1.HLTH_PROD_SRVC_TYPE_CD"
      ,$"FLT1.EMPLR_DEPT_NBR"
      ,$"FLT1.MAX_FUNDG_CF_CD"
      ,$"FLT1.SGMNTN_ASSN_ID"
      ,$"FLT1.SGMNTN_CLNT_ID"
      ,$"FULLY_INSRD_CD"
      ,$"FLT1.FUNDG_CF_CD"
      ,$"FLT1.src_grp_nbr"
      ,$"FLT1.mdcl_mbr_cvrg_cnt"
      ,$"FLT1.vsn_mbr_cnt"
      ,$"FLT1.dntl_mbr_cnt"
      ,$"FLT1.mbr_prod_enrlmnt_trmntn_dt"
      ,$"FLT1.cntrct_type_cd"
      ,$"FLT1.othr_insrnc_type_cd"
      ,$"FLT1.gndr_cd"
      ,$"FLT1.mbr_mnth_cob_cd"
      ,$"FLT1.prcp_id"
      ,$"FLT1.prcp_ctgry_cd"
      ,$"FLT1.prcp_type_cd"
      ,$"FLT1.hc_id"
      ,$"FLT1.sbscrbr_id"
      ,$"FLT1.scrty_lvl_cd"
      ,$"FLT1.prod_ofrg_key"
      ,$"FLT1.mdcl_expsr_nbr"
      ,$"FLT1.phrmcy_mbr_expsr_nbr"
      ,$"FLT1.dntl_expsr_nbr"
      ,$"FLT1.vsn_expsr_nbr"
      ,$"FLT1.PHRMCY_MBR_CVRG_CNT"
      ,$"FLT1.RX_OVERRIDE_FLAG"
      ,$"FLT1.ORIG_HLTH_PROD_SRVC_TYPE_CD"
      ,$"FLT1.MBU_CF_CD"
      ,$"FLT1.CLM_CD"
      ,$"FLT1.CLM_RPTG_1_CD"
      ,$"FLT1.CLM_RPTG_2_CD"
      ,$"FLT1.CLM_RPTG_3_CD"
      ,$"FLT1.EMP_NBR"
      ,$"FLT1.PLAN_GRP_ID"
      ,$"FLT1.CDHP_CTGRY_CD"
      ,$"FLT1.MBR_NTWK_ID"
      ,$"PRMRY_CVRG_CD"
      ,$"MEDCR_CD"
      ,$"FLT1.CII_RMM_FLG"
    )
// segmentation will add    PRMRY_CVRG_CD & MEDCR_CD logic here before acct_id is derived--> LOOK ABOVE

  val df_wrk_dt= df_wrk.withColumn("load_dtm", typedLit(current_timestamp).cast(TimestampType))
    .withColumn("load_log_key", lit(aDataSrcCfg.loadlogkey))


 /*



val SCD_RLTNSHP= getSparkSession.table(s"${dbname}.SCD_ORG_RLTNSHP").as("SCD")
          .join(df_RLTNSHP.as("RLTNSHP"), $"RLTNSHP.ACCT_ID" === $"SCD.SCD_ORG_ID")
          .where(trim($"SCD.SCD_ORG_TYPE_CD")==="WAC")
          .where($"SCD.RLTD_SCD_ORG_TYPE_CD".isin("GRP","CLNT","ASSN"))
          .select(trim($"SCD.RLTD_SCD_ORG_SOR_CD").as("RLTD_SCD_ORG_SOR_CD"),trim($"SCD.RLTD_SCD_ORG_ID").as("RLTD_SCD_ORG_ID"),  trim($"SCD.SCD_ORG_ID").as("SCD_ORG_ID"),trim($"SCD.RLTD_SCD_ORG_TYPE_CD").as("RLTD_SCD_ORG_TYPE_CD"))


        val  SCD_RLTNSHP_GRP=broadcast(SCD_RLTNSHP.filter($"RLTD_SCD_ORG_TYPE_CD"=== "GRP"))
         val  SCD_RLTNSHP_CLNT=broadcast(SCD_RLTNSHP.filter($"RLTD_SCD_ORG_TYPE_CD"=== "CLNT"))
          val  SCD_RLTNSHP_ASSN=broadcast(SCD_RLTNSHP.filter($"RLTD_SCD_ORG_TYPE_CD"=== "ASSN"))


val FRAME4_1= df_wrk.as("F3").join(SCD_RLTNSHP_GRP.as("SCD1"),$"F3.SGMNTN_SOR_CD" === $"SCD1.RLTD_SCD_ORG_SOR_CD"
            && $"F3.SGMNTN_GRP_ID" === $"SCD1.RLTD_SCD_ORG_ID").filter($"cii_rmm_flg"=== "Y")

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
              ,$"F3.MEDCR_CD"
              ,$"F3.CII_RMM_FLG")
	      
    
	   

val FRAME4_2= df_wrk.as("F3") .join(SCD_RLTNSHP_CLNT.as("SCD2"),$"F3.SGMNTN_SOR_CD" === $"SCD2.RLTD_SCD_ORG_SOR_CD"
            && $"F3.SGMNTN_CLNT_ID" === $"SCD2.RLTD_SCD_ORG_ID").filter($"cii_rmm_flg"=== "Y").select(
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
              ,$"F3.MEDCR_CD"
              ,$"F3.CII_RMM_FLG")
	      
  
	        
val FRAME4_3= df_wrk.as("F3").join(SCD_RLTNSHP_ASSN.as("SCD3"),$"F3.SGMNTN_SOR_CD" === $"SCD3.RLTD_SCD_ORG_SOR_CD"
            && $"F3.SGMNTN_ASSN_ID" === $"SCD3.RLTD_SCD_ORG_ID").filter($"cii_rmm_flg"=== "Y").select(
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
             ,$"F3.MEDCR_CD"
             ,$"F3.CII_RMM_FLG")
	     
   
	     
	         
val FRAME4 =  FRAME4_1.unionAll(FRAME4_2).unionAll(FRAME4_3)




val scd_org= getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(4))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)) //scd_org_scrty.sql

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
            ,$"F4.MEDCR_CD"
            ,when($"srg.ACCT_ID".isNull,'N').otherwise($"F4.CII_RMM_FLG").as("CII_RMM_FLG")).distinct()
            
           
              
              
              
              //val FRAME5=CIIUtilities.readDFfromS3(aDataSrcCfg, "FRAME5_combined_new")
                 
            
val FRAME6_1= FRAME5.as("F4").
				filter($"cii_rmm_flg"=== 'Y' || ($"cii_rmm_flg"=== 'N' && $"MDCL_EXPSR_NBR" >0))
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
	                          ,$"F4.prod_id"
	                          ,$"F4.PLAN_ID"
	                          ,$"F4.SGMNTN_SOR_CD"
	                         ,$"RPTG_OFSHR_ACSBL_IND"
	                          ,$"SCRTY_LVL_CD"
	                        ,$"mbr_key"
							,$"elgblty_clndr_mnth_end_dt"
							,$"mbrshp_sor_cd"
							,$"src_grp_nbr"
							,$"rltd_prchsr_org_nbr"
							,$"prchsr_org_nbr"
							,$"fundg_cf_cd"
							,$"prod_sor_cd"
							,$"prod_id_fltr"
							,$"bnft_pkg_id"
							,$"pkg_nbr"
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
							,$"scrty_lvl_cd_fltr"
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
            ,$"F4.MEDCR_CD"
            ,$"CII_RMM_FLG").distinct()
            
            
            
            
            val FRAME6_2=df_wrk.as("F4").filter($"cii_rmm_flg"=== 'N')
					.select(lit("NA").as("ACCT_ID")
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
	                         ,lit("N").as("RPTG_OFSHR_ACSBL_IND")
	                          ,lit("N").as("SCRTY_LVL_CD")
	                        ,$"mbr_key"
							,$"elgblty_clndr_mnth_end_dt"
							,$"mbrshp_sor_cd"
							,$"src_grp_nbr"
							,$"rltd_prchsr_org_nbr"
							,$"SGMNTN_GRP_ID".as("prchsr_org_nbr")
							,$"fundg_cf_cd"
							,$"prod_sor_cd"
							,$"prod_id".as("prod_id_fltr")
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
							,lit("N").as("scrty_lvl_cd_fltr")
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
            ,$"F4.MEDCR_CD"
            ,$"CII_RMM_FLG").distinct()
            
            
         
              
   
                       
     val FRAME6 =  FRAME6_1.unionAll(FRAME6_2)      

  CIIUtilities.writeDFtoS3(aDataSrcCfg,FRAME6,"FRAME6")

  val frame5_new =CIIUtilities.readDFfromS3(aDataSrcCfg,"FRAME6")

  frame5_new.createOrReplaceTempView("xwalk_stg")

  val mstr_sgmnt_elgbl= broadcast(getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(7))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer))) //ERSU

  mstr_sgmnt_elgbl.createOrReplaceTempView("mstr_sgmnt_elgbl")

  val WORK_GRP_SGMNTN_USED_tbl= broadcast(getSparkSession.table(s"${dbname}.WORK_GRP_SGMNTN_USED"))

  WORK_GRP_SGMNTN_USED_tbl.createOrReplaceTempView("WORK_GRP_SGMNTN_USED_tbl")

  val Final_Ersu_1= getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(8))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)) //ERSU
  val Final_Ersu_Final= Final_Ersu_1.withColumn("load_dtm", typedLit(current_timestamp).cast(TimestampType))
    .withColumn("load_log_key", lit(aDataSrcCfg.loadlogkey))


   */

  val df_sgmntn_mbr_glbl= castColumnTypes(df_wrk_dt, aSchemaDef)

  df_sgmntn_mbr_glbl

 }
}