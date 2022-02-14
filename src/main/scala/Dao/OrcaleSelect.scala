package Dao

import org.apache.spark.sql.SparkSession
//clickhouse date64字段类型不支持1970年之前数据需要转换为1970年数据
object OrcaleSelect {

  def sql13(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        | 'Y' SBLX,
        |	case
        |   when zhdq.BDZID is not null then nvl(t4.WZDWBM,zhdq.BDZID)
        |   when zhdq.XSDW_ID is not null then nvl(t2.WZDWBM,zhdq.XSDW_ID)
        |   when zhdq.JC_ID is not null then nvl(t3.WZDWBM,zhdq.JC_ID)
        | end as wzdwbm,
        |	b1.AZWZDM,
        |	b1.AZWZMC,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	b1.BDYY,
        |	zhdq.CCBH,
        |	to_unix_timestamp(b1.CCRQ) CCRQ,
        |	zhdq.DDDW,
        |	b1.DYDJ,
        |	b1.ID,
        |	zhdq.JC_ID,
        |	zhdq.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	zhdq.XSDW_ID,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	zhdq.ZCSX,
        |	b1.RERUN_FLAG,
        |	zhdq.ZCSX_ID,
        |	zhdq.DDDW_ID,
        |	zhdq.SZDW_ID,
        |	zhdq.XHGGID XHGG_ID,
        |	zhdq.ZZDWID SJZZDWID,
        |	zhdq.BDZID BDZ_ID,
        |	b1.data_type,
        |	b5.XHGGDM XHGG,
        |	b6.QYMC SJZZDWMC,
        | b6.QYDM SJZZDWLX,
        |	zhdq.SBLY,
        | t1.ID as yxid,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        | CASE
        |    	WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		ELSE t1.ZYCXSJ
        | END AS ZYCXSJ,
        | CASE
        |		WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		WHEN t1.ZTFL = 'LO' THEN NULL
        |		ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	t4.DWMC AS BDZMC,
        |	t4.BJDWBM AS BDZDM,
        |	t1.ZC_ID,
        |	t5.AZWZDM AS WZXLDM,
        |	t5.AZWZMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |	end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC,
        | zhdq.sblx AS ZHSBLX,
        | b1.jgid as ZHDQJGID,
        | b1.zhdqid as ZHDQID
        | ,b1.lx as YJLX
        |	,e.empty BDRQ
        |    ,e.empty DLGS
        |    ,e.empty DLZDT
        |    ,e.empty DLZJT
        |    ,e.empty DTCCRQ
        |    ,e.empty DTZZC
        |    ,e.empty DTZZC_ID
        |    ,e.empty EDDL
        |    ,e.empty JCRQ
        |    ,e.empty JXFS
        |    ,e.empty JYCCRQ
        |    ,e.empty JYCLJS
        |    ,e.empty JYZZC
        |    ,e.empty JYZZC_ID
        |    ,e.empty MXXS
        |    ,e.empty MXXS_ID
        |    ,e.empty PDBJ
        |    ,e.empty QYBM
        |    ,e.empty QYJB
        |    ,e.empty QYLX
        |    ,e.empty REMARK
        |    ,e.empty SBXS
        |    ,e.empty SBXS_ID
        |    ,b1.sbid SB_ID
        |    ,e.empty SDBZ
        |    ,e.empty SGDW
        |    ,e.empty SGDW_ID
        |    ,e.empty TZ
        |    ,e.empty XLCD
        |    ,e.empty XLDM
        |    ,e.empty XLFDS
        |    ,e.empty XLLB
        |    ,e.empty XLMC
        |    ,e.empty XLXZ
        |    ,e.empty XL_ID
        |    ,e.empty YXZDDL
        |    ,e.empty YZ
        |    ,e.empty ZCD
        |    ,e.empty ZSPL
        |    ,t1.ismerge ISMERGE
        |    ,to_unix_timestamp(jg.CHANGEZCRQ) JGZCRQ
        |    ,to_unix_timestamp(jg.CHANGETCRQ) JGTCRQ
        |    ,to_unix_timestamp(jg.CHANGETUIYIRQ) JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,jg.azwzdm JG_AZWZDM
        |    ,jg.azwzmc JG_AZWZMC
        |    ,zhdq.azwzdm ZHDQ_AZWZDM
        |    ,zhdq.azwzmc ZHDQ_AZWZMC
        |    ,to_unix_timestamp(zhdq.CHANGEZCRQ) ZHDQZCRQ
        |    ,to_unix_timestamp( zhdq.CHANGETCRQ) ZHDQTCRQ
        |    ,to_unix_timestamp(zhdq.CHANGETUIYIRQ) ZHDQTUIYIRQ
        |FROM
        |	ZC_ZHDQ_JG_YJ b1
        |	left join ZC_ZHDQ_JG jg
        |	on b1.JGID=jg.ID
        |	left join ZC_ZHDQ zhdq
        |    on b1.zhdqid=zhdq.ID
        |	LEFT JOIN YX_ZHDQ t1
        |       ON b1.id = t1.zc_id
        |       and t1.data_type = '2'
        |	     AND t1.del_status = '1'
        |      and zhdq.JC_ID=t1.JC_ID
        |      and zhdq.XSDW_ID=t1.XSDW_ID
        |      and zhdq.BDZID=t1.BDZDM_ID
        |      and zhdq.dydj=t1.dydj
        |	LEFT JOIN DM_XSDW t2 ON zhdq.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC t3 ON zhdq.JC_ID = t3.ID
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN DM_BDZ t4 ON zhdq.BDZID = t4.ID
        |	LEFT JOIN ZC_ZBQ t5 ON t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ t6 ON t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB t7 ON t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY t8 ON t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY t9 ON t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY t10 ON t1.ZRYYDMF_ID = t10.ID
        |	LEFT JOIN DM_XHGG b5 ON b5.ID = zhdq.XHGGID
        |	LEFT JOIN (select * from DM_QY where del_status = '1') b6 on b6.ID = zhdq.ZZDWID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1'  and b1.data_type = '2'
        |""".stripMargin)
  }
  def sql12(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |	'C' SBLX,
        | case
        |   when b1.BDZ_ID is not null then nvl(t4.WZDWBM,b1.BDZ_ID)
        |   when b1.XSDW_ID is not null then nvl(t2.WZDWBM,b1.XSDW_ID)
        |   when b1.JC_ID is not null then nvl(t3.WZDWBM,b1.JC_ID)
        | end as wzdwbm,
        |	b1.AZWZDM,
        |	b1.AZWZMC,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.DDDW,
        |	to_unix_timestamp(b1.DTCCRQ) DTCCRQ,
        |	b1.DYDJ,
        |	b1.EDDL,
        |	b1.ID,
        |	b1.JC_ID,
        |	to_unix_timestamp(b1.JYCCRQ) JYCCRQ,
        |	b1.JYCLJS,
        |	b1.MXXS,
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.XSDW_ID,
        |	b1.ZCD,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        |	b1.RERUN_FLAG,
        |	b1.JYZZC_ID,
        |	b1.MXXS_ID,
        |	b1.BDZ_ID,
        |	b1.ZCSX_ID,
        |	b1.DDDW_ID,
        |	b1.SZDW_ID,
        |	b1.XHGG_ID,
        |	b1.DTZZC_ID,
        |	b1.data_type,
        | b5.XHGGDM XHGG,
        |	b6.QYMC   SGDW ,
        | b6.ID     SGDW_ID,
        |	b7.SSXSMC JXFS,
        |	b8.QYMC   DTZZC,
        |	b9.QYMC   JYZZC,
        |	b1.SBLY,
        |	t1.ID AS YXID,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        |	 CASE
        |    	WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		ELSE t1.ZYCXSJ
        |    END AS ZYCXSJ,
        |    CASE
        |		WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		WHEN t1.ZTFL = 'LO' THEN NULL
        |		ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	t4.DWMC AS BDZMC,
        |	t4.BJDWBM AS BDZDM,
        |	t1.ZC_ID,
        |	t5.AZWZDM AS WZXLDM,
        |	t5.AZWZMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |		end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC
        |    ,e.empty YJLX
        |    ,e.empty as ZHDQJGID
        |    ,e.empty as ZHDQID
        |    ,e.empty CCBH
        |    ,e.empty CCRQ
        |    ,e.empty DLGS
        |    ,e.empty DLZDT
        |    ,e.empty DLZJT
        |    ,e.empty JCRQ
        |    ,e.empty PDBJ
        |    ,e.empty QYBM
        |    ,e.empty QYJB
        |    ,e.empty SBXS
        |    ,e.empty SBXS_ID
        |    ,e.empty TZ
        |    ,b1.EDDL XLCD
        |    ,e.empty XLDM
        |    ,e.empty XLFDS
        |    ,e.empty XLLB
        |    ,e.empty XLMC
        |    ,e.empty XLXZ
        |    ,e.empty XL_ID
        |    ,e.empty YXZDDL
        |    ,e.empty YZ
        |    ,e.empty ZSPL
        |    ,e.empty SJZZDWMC
        |    ,e.empty SJZZDWLX
        |    ,b1.SGDW_ID SJZZDWID
        |    ,e.empty ZHSBLX
        |    ,e.empty ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |    ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM       ZC_MX     b1
        |	LEFT JOIN YX_MX     t1    ON           b1.id = t1.zc_id
        |                          and    t1.data_type = '2'
        |	                         AND   t1.del_status = '1'
        |                          and        b1.JC_ID = t1.JC_ID
        |                          and      b1.XSDW_ID = t1.XSDW_ID
        |                          and       b1.BDZ_ID = t1.BDZDM_ID
        |                          and         b1.dydj = t1.dydj
        |	LEFT JOIN DM_XSDW   t2    ON      b1.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC     t3    ON        b1.JC_ID = t3.ID
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN DM_BDZ    t4    ON       b1.BDZ_ID = t4.ID
        |	LEFT JOIN ZC_MX     t5    ON        t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ     t6    ON         t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB     t7    ON    t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY   t8    ON    t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY   t9    ON    t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY   t10   ON   t1.ZRYYDMF_ID = t10.ID
        |	LEFT JOIN DM_XHGG   b5    ON           b5.ID = b1.XHGG_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1')     b6    ON           b6.ID = b1.SGDW_ID
        |	LEFT JOIN DM_SSXS   b7    ON           b7.ID = b1.JXFS_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1')     b8    ON           b8.ID = b1.DTZZC_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1')     b9    ON           b9.ID = b1.JYZZC_ID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1'  and b1.data_type = '2'
        |""".stripMargin)
  }

  def sql11(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |	'A' SBLX,
        |	case
        |    when  b1.XSDW_ID is not null then nvl(t2.WZDWBM,b1.XSDW_ID)
        |    when  b1.JC_ID is not null then nvl(t3.WZDWBM,b1.JC_ID)
        | end as wzdwbm,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.DDDW,
        |	b1.dddw_id,
        |	b1.DLGS,
        |	b1.DLZDT,
        |	b1.QYBM,
        |	b1.DLZJT,
        |	b1.DYDJ,
        |	b1.ID,
        |	to_unix_timestamp(b1.JCRQ) JCRQ,
        |	b1.JC_ID,
        |	b1.PDBJ,
        |	b1.QYJB,
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	b1.szdw_id,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.XLCD,
        |	b1.XLLB,
        |	b1.XLFDS,
        |	b1.XLXZ,
        |	b1.XSDW_ID,
        |	b1.YXZDDL,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        |	b1.zcsx_id,
        |	b1.ZSPL,
        |	b1.RERUN_FLAG,
        |	b1.ZZDW_ID SJZZDWID,
        |	concat (b1.XLSSDW,b1.XLDM ) XL_ID,
        |	b1.data_type,
        |	b4.QYMC SJZZDWMC,
        | b4.QYDM SJZZDWLX,
        |	b5.QYMC SGDW,
        | b5.ID   SGDW_ID,
        |	b6.XLDM XLDM,
        |	b6.XLMC XLMC,
        |	b1.SBLY,
        |	t1.ID AS YXID,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        |    CASE
        |    	WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		ELSE t1.ZYCXSJ
        |    END AS ZYCXSJ,
        |    CASE
        |		WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		WHEN t1.ZTFL = 'LO' THEN NULL
        |		ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	e.empty AS BDZMC,
        |	e.empty AS BDZDM,
        |	t1.ZC_ID,
        |	t5.XLDM AS WZXLDM,
        |	t5.XLMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |		end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC
        |    ,e.empty as ZHDQJGID
        |    ,e.empty YJLX
        |    ,e.empty as ZHDQID
        |    ,e.empty AZWZDM
        |    ,e.empty AZWZMC
        |    ,e.empty BDZ_ID
        |    ,e.empty CCBH
        |    ,e.empty CCRQ
        |    ,e.empty DTCCRQ
        |    ,e.empty DTZZC
        |    ,e.empty DTZZC_ID
        |    ,e.empty EDDL
        |    ,e.empty JXFS
        |    ,e.empty JYCCRQ
        |    ,e.empty JYCLJS
        |    ,e.empty JYZZC
        |    ,e.empty JYZZC_ID
        |    ,e.empty MXXS
        |    ,e.empty MXXS_ID
        |    ,e.empty SBXS
        |    ,e.empty SBXS_ID
        |    ,e.empty TZ
        |    ,e.empty XHGG
        |    ,e.empty XHGG_ID
        |    ,e.empty YZ
        |    ,e.empty ZCD
        |    ,e.empty ZHSBLX
        |    ,t1.ismerge ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |     ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM
        |	ZC_DLXL b1
        |	LEFT JOIN YX_DLXL t1 ON b1.id = t1.zc_id
        |	      AND t1.del_status = '1'
        |       and t1.data_type = '2'
        |       and b1.JC_ID=t1.JC_ID
        |       and b1.XSDW_ID=t1.XSDW_ID
        |       and b1.dydj=t1.dydj
        |	LEFT JOIN DM_XSDW t2 ON b1.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC t3 ON b1.JC_ID = t3.ID
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN ZC_DLXL t5 ON t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ t6 ON t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB t7 ON t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY t8 ON t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY t9 ON t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY t10 ON t1.ZRYYDMF_ID = t10.ID
        |	LEFT JOIN DM_XL b6 ON b6.ID = b1.XL_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1') b5 ON b5.ID = b1.SGDW_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1') b4 ON b4.ID = b1.ZZDW_ID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1' and b1.data_type = '2'
        |""".stripMargin)
  }

  def sql10(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        | '9' SBLX,
        | case
        |   when b1.BDZ_ID is not null then nvl(t4.WZDWBM,b1.BDZ_ID)
        |   when b1.XSDW_ID is not null then nvl(t2.WZDWBM,b1.XSDW_ID)
        |   when b1.JC_ID is not null then nvl(t3.WZDWBM,b1.JC_ID)
        | end as wzdwbm,
        |	b1.AZWZDM,
        |	b1.AZWZMC,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.CCBH,
        |	to_unix_timestamp(b1.CCRQ) CCRQ,
        |	b1.DDDW,
        |	b1.DYDJ,
        |	b1.ID,
        |	b1.JC_ID,
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.XSDW_ID,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        |	b1.ZSPL,
        |	b1.RERUN_FLAG,
        |	b1.ZCSX_ID,
        |	b1.DDDW_ID,
        |	b1.SZDW_ID,
        |	b1.XHGG_ID,
        |	b1.ZZDW_ID SJZZDWID,
        |	b1.BDZ_ID,
        |	b1.data_type,
        |	b5.XHGGDM XHGG,
        |	b6.QYMC SJZZDWMC,
        | b6.QYDM SJZZDWLX,
        |	b1.SBLY,
        |	t1.ID AS YXID,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        | CASE
        |   WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		ELSE t1.ZYCXSJ
        | END AS ZYCXSJ,
        | CASE
        |		WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		WHEN t1.ZTFL = 'LO' THEN NULL
        |		ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	t4.DWMC AS BDZMC,
        |	t4.BJDWBM AS BDZDM,
        |	t1.ZC_ID,
        |	t5.AZWZDM AS WZXLDM,
        |	t5.AZWZMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |		end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC
        |    ,e.empty as ZHDQJGID
        |    ,e.empty YJLX
        |    ,e.empty as ZHDQID
        |    ,e.empty DLGS
        |    ,e.empty DLZDT
        |    ,e.empty DLZJT
        |    ,e.empty DTCCRQ
        |    ,e.empty DTZZC
        |    ,e.empty DTZZC_ID
        |    ,e.empty EDDL
        |    ,e.empty JCRQ
        |    ,e.empty JXFS
        |    ,e.empty JYCCRQ
        |    ,e.empty JYCLJS
        |    ,e.empty JYZZC
        |    ,e.empty JYZZC_ID
        |    ,e.empty MXXS
        |    ,e.empty MXXS_ID
        |    ,e.empty PDBJ
        |    ,e.empty QYBM
        |    ,e.empty QYJB
        |    ,e.empty SBXS
        |    ,e.empty SBXS_ID
        |    ,e.empty SGDW
        |    ,e.empty SGDW_ID
        |    ,e.empty TZ
        |    ,b1.ZK XLCD
        |    ,e.empty XLDM
        |    ,e.empty XLFDS
        |    ,e.empty XLLB
        |    ,e.empty XLMC
        |    ,e.empty XLXZ
        |    ,e.empty XL_ID
        |    ,e.empty YXZDDL
        |    ,e.empty YZ
        |    ,e.empty ZCD
        |    ,e.empty ZHSBLX
        |    ,e.empty ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |    ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM
        |	ZC_ZBQ b1
        |	LEFT JOIN YX_ZBQ t1 ON b1.id = t1.zc_id
        |	      AND t1.del_status = '1'
        |       and t1.data_type = '2'
        |       and b1.JC_ID=t1.JC_ID
        |       and b1.XSDW_ID=t1.XSDW_ID
        |       and b1.BDZ_ID=t1.BDZDM_ID
        |       and b1.dydj=t1.dydj
        |	LEFT JOIN DM_XSDW t2 ON b1.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC t3 ON b1.JC_ID = t3.ID
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN DM_BDZ t4 ON b1.BDZ_ID = t4.ID
        |	LEFT JOIN ZC_ZBQ t5 ON t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ t6 ON t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB t7 ON t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY t8 ON t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY t9 ON t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY t10 ON t1.ZRYYDMF_ID = t10.ID
        |	LEFT JOIN DM_XHGG b5 on b5.ID = b1.XHGG_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1') b6 on b6.ID = b1.ZZDW_ID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1' and b1.data_type = '2'
        |""".stripMargin)
  }

  def sql9(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        | '8' SBLX,
        | case
        |   when b1.BDZ_ID is not null then nvl(t4.WZDWBM,b1.BDZ_ID)
        |   when b1.XSDW_ID is not null then nvl(t2.WZDWBM,b1.XSDW_ID)
        |   when b1.JC_ID is not null then nvl(t3.WZDWBM,b1.JC_ID)
        | end as wzdwbm,
        |	b1.AZWZDM,
        |	b1.AZWZMC,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.CCBH,
        |	to_unix_timestamp(b1.CCRQ) CCRQ,
        |	b1.DDDW,
        |	b1.DYDJ,
        |	b1.ID,
        |	b1.JC_ID,
        |	b1.PDBJ,
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.XSDW_ID,
        |	b1.YZ,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        |	b1.RERUN_FLAG,
        |	b1.DDDW_ID,
        |	b1.SZDW_ID,
        |	b1.XHGG_ID,
        |	b1.ZZDW_ID SJZZDWID,
        |	b1.ZCSX_ID,
        |	b1.BDZ_ID,
        |	b1.data_type,
        |	b5.XHGGDM XHGG,
        |	b6.QYMC SJZZDWMC,
        | b6.QYDM SJZZDWLX,
        |	b1.SBLY,
        |	t1.ID AS YXID,
        | to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        |    CASE
        |    	WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		ELSE t1.ZYCXSJ
        |    END AS ZYCXSJ,
        |    CASE
        |		WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		WHEN t1.ZTFL = 'LO' THEN NULL
        |		ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	t4.DWMC AS BDZMC,
        |	t4.BJDWBM AS BDZDM,
        |	t1.ZC_ID,
        |	t5.AZWZDM AS WZXLDM,
        |	t5.AZWZMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |		end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC
        |    ,e.empty as ZHDQJGID
        |    ,e.empty YJLX
        |    ,e.empty as ZHDQID
        |    ,e.empty DLGS
        |    ,e.empty DLZDT
        |    ,e.empty DLZJT
        |    ,e.empty DTCCRQ
        |    ,e.empty DTZZC
        |    ,e.empty DTZZC_ID
        |    ,e.empty EDDL
        |    ,e.empty JCRQ
        |    ,e.empty JXFS
        |    ,e.empty JYCCRQ
        |    ,e.empty JYCLJS
        |    ,e.empty JYZZC
        |    ,e.empty JYZZC_ID
        |    ,e.empty MXXS
        |    ,e.empty MXXS_ID
        |    ,e.empty QYBM
        |    ,e.empty QYJB
        |    ,e.empty SBXS
        |    ,e.empty SBXS_ID
        |    ,e.empty SGDW
        |    ,e.empty SGDW_ID
        |    ,e.empty TZ
        |    ,b1.DRL XLCD
        |    ,e.empty XLDM
        |    ,e.empty XLFDS
        |    ,e.empty XLLB
        |    ,e.empty XLMC
        |    ,e.empty XLXZ
        |    ,e.empty XL_ID
        |    ,e.empty YXZDDL
        |    ,e.empty ZCD
        |    ,e.empty ZSPL
        |    ,e.empty ZHSBLX
        |    ,e.empty ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |    ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM
        |	ZC_OHDRQ b1
        |	LEFT JOIN YX_OHDRQ t1 ON b1.id = t1.zc_id
        |	      AND t1.del_status = '1'
        |       and t1.data_type = '2'
        |       and b1.JC_ID=t1.JC_ID
        |       and b1.XSDW_ID=t1.XSDW_ID
        |       and b1.BDZ_ID=t1.BDZDM_ID
        |       and b1.dydj=t1.dydj
        |	LEFT JOIN DM_XSDW t2 ON b1.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC t3 ON b1.JC_ID = t3.ID
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN DM_BDZ t4 ON b1.BDZ_ID = t4.ID
        |	LEFT JOIN ZC_OHDRQ t5 ON t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ t6 ON t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB t7 ON t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY t8 ON t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY t9 ON t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY t10 ON t1.ZRYYDMF_ID = t10.ID
        |	LEFT JOIN DM_XHGG b5 on b5.ID = b1.XHGG_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1') b6 on b6.ID = b1.ZZDW_ID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1' and b1.data_type = '2'
        |""".stripMargin)
  }

  def sql8(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        | '2' SBLX,
        | case
        |   when b1.BDZ_ID is not null then nvl(t4.WZDWBM,b1.BDZ_ID)
        |   when b1.XSDW_ID is not null then nvl(t2.WZDWBM,b1.XSDW_ID)
        |   when b1.JC_ID is not null then nvl(t3.WZDWBM,b1.JC_ID)
        | end as wzdwbm,
        |	b1.AZWZDM,
        |	b1.AZWZMC,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.CCBH,
        |	to_unix_timestamp(b1.CCRQ) CCRQ,
        |	b1.DDDW,
        |	b1.DYDJ,
        |	b1.EDDL,
        |	b1.ID,
        |	b1.JC_ID,
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.TZ,
        |	b1.XSDW_ID,
        |	b1.YZ,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        |	b1.RERUN_FLAG,
        |	b1.BDZ_ID,
        |	b1.data_type,
        |	b1.ZCSX_ID,
        |	b1.DDDW_ID,
        |	b1.SZDW_ID,
        |	b1.XHGG_ID,
        |	b1.ZZDW_ID SJZZDWID ,
        |	b5.XHGGDM XHGG,
        |	b6.QYMC SJZZDWMC,
        | b6.QYDM SJZZDWLX,
        |	b1.SBLY ,
        |	t1.ID AS YXID,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        |    CASE
        |    	WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		ELSE t1.ZYCXSJ
        |    END AS ZYCXSJ,
        |    CASE
        |		WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		WHEN t1.ZTFL = 'LO' THEN NULL
        |		ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	t4.DWMC AS BDZMC,
        |	t4.BJDWBM AS BDZDM,
        |	t1.ZC_ID,
        |	t5.AZWZDM AS WZXLDM,
        |	t5.AZWZMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |		end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC
        |    ,e.empty as ZHDQJGID
        |    ,e.empty as ZHDQID
        |    ,e.empty YJLX
        |    ,e.empty DLGS
        |    ,e.empty DLZDT
        |    ,e.empty DLZJT
        |    ,e.empty DTCCRQ
        |    ,e.empty DTZZC
        |    ,e.empty DTZZC_ID
        |    ,e.empty JCRQ
        |    ,e.empty JXFS
        |    ,e.empty JYCCRQ
        |    ,e.empty JYCLJS
        |    ,e.empty JYZZC
        |    ,e.empty JYZZC_ID
        |    ,e.empty MXXS
        |    ,e.empty MXXS_ID
        |    ,e.empty PDBJ
        |    ,e.empty QYBM
        |    ,e.empty QYJB
        |    ,e.empty SBXS
        |    ,e.empty SBXS_ID
        |    ,e.empty SGDW
        |    ,e.empty SGDW_ID
        |    ,b1.RL XLCD
        |    ,e.empty XLDM
        |    ,e.empty XLFDS
        |    ,e.empty XLLB
        |    ,e.empty XLMC
        |    ,e.empty XLXZ
        |    ,e.empty XL_ID
        |    ,e.empty YXZDDL
        |    ,e.empty ZCD
        |    ,e.empty ZSPL
        |    ,e.empty ZHSBLX
        |    ,e.empty ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |    ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM
        |	ZC_DKQ b1
        |	LEFT JOIN YX_DKQ t1 ON b1.id = t1.zc_id
        |     	AND t1.del_status = '1'
        |      and t1.data_type = '2'
        |       and b1.JC_ID=t1.JC_ID
        |       and b1.XSDW_ID=t1.XSDW_ID
        |       and b1.BDZ_ID=t1.BDZDM_ID
        |       and b1.dydj=t1.dydj
        |	LEFT JOIN DM_XSDW t2 ON b1.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC t3 ON b1.JC_ID = t3.ID
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN DM_BDZ t4 ON b1.BDZ_ID = t4.ID
        |	LEFT JOIN ZC_BLQ t5 ON t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ t6 ON t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB t7 ON t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY t8 ON t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY t9 ON t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY t10 ON t1.ZRYYDMF_ID = t10.ID
        |	LEFT JOIN DM_XHGG b5 on b5.ID = b1.XHGG_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1') b6 on b6.ID = b1.ZZDW_ID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1' and b1.data_type = '2'
        |""".stripMargin)
  }

  def sql7(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        | '7' SBLX,
        | case
        |   when b1.BDZ_ID is not null then nvl(t4.WZDWBM,b1.BDZ_ID)
        |   when b1.XSDW_ID is not null then nvl(t2.WZDWBM,b1.XSDW_ID)
        |   when b1.JC_ID is not null then nvl(t3.WZDWBM,b1.JC_ID)
        | end as wzdwbm,
        |	b1.AZWZDM,
        |	b1.AZWZMC,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.CCBH,
        |	to_unix_timestamp(b1.CCRQ) CCRQ,
        |	b1.DDDW,
        |	b1.DYDJ,
        |	b1.ID,
        |	b1.JC_ID,
        |	b1.PDBJ,
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.XSDW_ID,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        |	b1.RERUN_FLAG,
        |	b1.DDDW_ID,
        |	b1.SZDW_ID,
        |	b1.XHGG_ID,
        |	b1.ZZDW_ID SJZZDWID ,
        |	b1.SBXS_ID,
        |	b1.ZCSX_ID,
        |	b1.BDZ_ID,
        |	b1.data_type,
        |	b5.XHGGDM XHGG,
        |	b6.QYMC SJZZDWMC,
        | b6.QYDM SJZZDWLX,
        |	b7.SSXSMC SBXS,
        |	b1.SBLY,
        |	t1.ID AS YXID,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        | CASE
        |   WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		ELSE t1.ZYCXSJ
        | END AS ZYCXSJ,
        | CASE
        |		WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		WHEN t1.ZTFL = 'LO' THEN NULL
        |		ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	t4.DWMC AS BDZMC,
        |	t4.BJDWBM AS BDZDM,
        |	t1.ZC_ID,
        |	t5.AZWZDM AS WZXLDM,
        |	t5.AZWZMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |		end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC
        |	   ,e.empty as ZHDQJGID
        |    ,e.empty YJLX
        |    ,e.empty as ZHDQID
        |    ,e.empty DLGS
        |    ,e.empty DLZDT
        |    ,e.empty DLZJT
        |    ,e.empty DTCCRQ
        |    ,e.empty DTZZC
        |    ,e.empty DTZZC_ID
        |    ,e.empty EDDL
        |    ,e.empty JCRQ
        |    ,e.empty JXFS
        |    ,e.empty JYCCRQ
        |    ,e.empty JYCLJS
        |    ,e.empty JYZZC
        |    ,e.empty JYZZC_ID
        |    ,e.empty MXXS
        |    ,e.empty MXXS_ID
        |    ,e.empty QYBM
        |    ,e.empty QYJB
        |    ,e.empty SGDW
        |    ,e.empty SGDW_ID
        |    ,e.empty TZ
        |    ,b1.EDRL XLCD
        |    ,e.empty XLDM
        |    ,e.empty XLFDS
        |    ,e.empty XLLB
        |    ,e.empty XLMC
        |    ,e.empty XLXZ
        |    ,e.empty XL_ID
        |    ,e.empty YXZDDL
        |    ,e.empty YZ
        |    ,e.empty ZCD
        |    ,e.empty ZSPL
        |    ,e.empty ZHSBLX
        |    ,e.empty ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |    ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM
        |	ZC_BLQ b1
        |	LEFT JOIN YX_BLQ t1 ON b1.id = t1.zc_id
        |	      AND t1.del_status = '1'
        |       and t1.data_type = '2'
        |       and b1.JC_ID=t1.JC_ID
        |       and b1.XSDW_ID=t1.XSDW_ID
        |       and b1.BDZ_ID=t1.BDZDM_ID
        |       and b1.dydj=t1.dydj
        |	LEFT JOIN DM_XSDW t2 ON b1.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC t3 ON b1.JC_ID = t3.ID
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN DM_BDZ t4 ON b1.BDZ_ID = t4.ID
        |	LEFT JOIN ZC_BLQ t5 ON t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ t6 ON t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB t7 ON t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY t8 ON t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY t9 ON t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY t10 ON t1.ZRYYDMF_ID = t10.ID
        |	LEFT JOIN DM_XHGG b5 on b5.ID = b1.XHGG_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1') b6 on b6.ID = b1.ZZDW_ID
        |	LEFT JOIN DM_SSXS b7 on b7.ID = b1.SBXS_ID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1' and b1.data_type = '2'
        |""".stripMargin)
  }

  def sql6(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        | '6' SBLX,
        | case
        |   when b1.BDZ_ID is not null then nvl(t4.WZDWBM,b1.BDZ_ID)
        |   when b1.XSDW_ID is not null then nvl(t2.WZDWBM,b1.XSDW_ID)
        |   when b1.JC_ID is not null then nvl(t3.WZDWBM,b1.JC_ID)
        | end as wzdwbm,
        |	b1.AZWZDM,
        |	b1.AZWZMC,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.CCBH,
        |	to_unix_timestamp(b1.CCRQ) CCRQ,
        |	b1.DDDW,
        |	b1.DYDJ,
        |	b1.EDDL,
        |	b1.ID,
        |	b1.JC_ID,
        |	b1.PDBJ,
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.XSDW_ID,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        |	b1.RERUN_FLAG,
        |	b1.BDZ_ID,
        |	b1.ZCSX_ID,
        |	b1.DDDW_ID,
        |	b1.SZDW_ID,
        |	b1.XHGG_ID,
        |	b1.ZZDW_ID SJZZDWID ,
        |	b1.SBXS_ID,
        |	b1.data_type,
        |	b5.XHGGDM XHGG,
        |	b6.QYMC SJZZDWMC,
        | b6.QYDM SJZZDWLX,
        |	b1.SBLY,
        |	t1.ID AS YXID,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        | CASE
        |   WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		ELSE t1.ZYCXSJ
        |    END AS ZYCXSJ,
        |    CASE
        |		WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		WHEN t1.ZTFL = 'LO' THEN NULL
        |		ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	t4.DWMC AS BDZMC,
        |	t4.BJDWBM AS BDZDM,
        |	t1.ZC_ID,
        |	t5.AZWZDM AS WZXLDM,
        |	t5.AZWZMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |		end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC
        |    ,e.empty as ZHDQJGID
        |    ,e.empty as ZHDQID
        |	   ,e.empty DLGS
        |    ,e.empty DLZDT
        |    ,e.empty YJLX
        |    ,e.empty DLZJT
        |    ,e.empty DTCCRQ
        |    ,e.empty DTZZC
        |    ,e.empty DTZZC_ID
        |    ,e.empty JCRQ
        |    ,e.empty JXFS
        |    ,e.empty JYCCRQ
        |    ,e.empty JYCLJS
        |    ,e.empty JYZZC
        |    ,e.empty JYZZC_ID
        |    ,e.empty MXXS
        |    ,e.empty MXXS_ID
        |    ,e.empty QYBM
        |    ,e.empty QYJB
        |    ,e.empty SBXS
        |    ,e.empty SGDW
        |    ,e.empty SGDW_ID
        |    ,e.empty TZ
        |    ,b1.EDDL XLCD
        |    ,e.empty XLDM
        |    ,e.empty XLFDS
        |    ,e.empty XLLB
        |    ,e.empty XLMC
        |    ,e.empty XLXZ
        |    ,e.empty XL_ID
        |    ,e.empty YXZDDL
        |    ,e.empty YZ
        |    ,e.empty ZCD
        |    ,e.empty ZSPL
        |    ,e.empty ZHSBLX
        |    ,e.empty ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |    ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM
        |	ZC_GLKG b1
        |	LEFT JOIN YX_GLKG t1 ON b1.id = t1.zc_id
        |	      AND t1.del_status = '1'
        |       and b1.JC_ID=t1.JC_ID
        |       and t1.data_type = '2'
        |       and b1.XSDW_ID=t1.XSDW_ID
        |       and b1.BDZ_ID=t1.BDZDM_ID
        |       and b1.dydj=t1.dydj
        |	LEFT JOIN DM_XSDW t2 ON b1.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC t3 ON b1.JC_ID = t3.ID
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN DM_BDZ t4 ON b1.BDZ_ID = t4.ID
        |	LEFT JOIN ZC_GLKG t5 ON t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ t6 ON t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB t7 ON t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY t8 ON t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY t9 ON t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY t10 ON t1.ZRYYDMF_ID = t10.ID
        |	LEFT JOIN DM_XHGG b5 on b5.ID = b1.XHGG_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1') b6 on b6.ID = b1.ZZDW_ID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1' and b1.data_type = '2'
        |""".stripMargin)

  }

  def sql5(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        | '5' SBLX,
        | case
        |   when b1.BDZ_ID is not null then nvl(t4.WZDWBM,b1.BDZ_ID)
        |   when b1.XSDW_ID is not null then nvl(t2.WZDWBM,b1.XSDW_ID)
        |   when b1.JC_ID is not null then nvl(t3.WZDWBM,b1.JC_ID)
        | end as wzdwbm,
        |	b1.AZWZDM,
        |	b1.AZWZMC,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.CCBH,
        |	to_unix_timestamp(b1.CCRQ) CCRQ,
        |	b1.DDDW,
        |	b1.DYDJ,
        |	b1.ID,
        |	b1.JC_ID,
        |	b1.PDBJ,
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.TZ,
        |	b1.XSDW_ID,
        |	b1.YZ,
        | to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        |	b1.RERUN_FLAG,
        |	b1.BDZ_ID,
        |	b1.ZCSX_ID,
        |	b1.DDDW_ID,
        |	b1.SZDW_ID,
        |	b1.XHGG_ID,
        |	b1.ZZDW_ID SJZZDWID ,
        |	b1.SBXS_ID,
        |	b1.data_type,
        |	b5.XHGGDM XHGG,
        |	b6.QYMC SJZZDWMC,
        | b6.QYDM SJZZDWLX,
        |	b7.SSXSMC SBXS,
        |	b1.SBLY,
        |	t1.ID AS YXID,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        | CASE
        |   WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		ELSE t1.ZYCXSJ
        | END AS ZYCXSJ,
        | CASE
        |		WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		WHEN t1.ZTFL = 'LO' THEN NULL
        |		ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	t4.DWMC AS BDZMC,
        |	t4.BJDWBM AS BDZDM,
        |	t1.ZC_ID,
        |	t5.AZWZDM AS WZXLDM,
        |	t5.AZWZMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |	end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC
        |    ,e.empty as ZHDQJGID
        |    ,e.empty as ZHDQID
        |    ,e.empty DLGS
        |    ,e.empty DLZDT
        |    ,e.empty YJLX
        |    ,e.empty DLZJT
        |    ,e.empty DTCCRQ
        |    ,e.empty DTZZC
        |    ,e.empty DTZZC_ID
        |    ,e.empty EDDL
        |    ,e.empty JCRQ
        |    ,e.empty JXFS
        |    ,e.empty JYCCRQ
        |    ,e.empty JYCLJS
        |    ,e.empty JYZZC
        |    ,e.empty JYZZC_ID
        |    ,e.empty MXXS
        |    ,e.empty MXXS_ID
        |    ,e.empty QYBM
        |    ,e.empty QYJB
        |    ,e.empty SGDW
        |    ,e.empty SGDW_ID
        |    ,b1.EDRL XLCD
        |    ,e.empty XLDM
        |    ,e.empty XLFDS
        |    ,e.empty XLLB
        |    ,e.empty XLMC
        |    ,e.empty XLXZ
        |    ,e.empty XL_ID
        |    ,e.empty YXZDDL
        |    ,e.empty ZCD
        |    ,e.empty ZSPL
        |    ,e.empty ZHSBLX
        |    ,e.empty ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |    ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM
        |	ZC_DYHGQ b1
        |	LEFT JOIN YX_DYHGQ t1 ON b1.id = t1.zc_id
        |	      AND t1.del_status = '1'
        |       and t1.data_type = '2'
        |       and b1.JC_ID=t1.JC_ID
        |       and b1.XSDW_ID=t1.XSDW_ID
        |       and b1.BDZ_ID=t1.BDZDM_ID
        |       and b1.dydj=t1.dydj
        |	LEFT JOIN DM_XSDW t2 ON b1.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC t3 ON b1.JC_ID = t3.ID
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1'  and del_status = '1') d1  on  1=1
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2'  and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3'  and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4'  and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5'  and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN DM_BDZ t4 ON b1.BDZ_ID = t4.ID
        |	LEFT JOIN ZC_DYHGQ t5 ON t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ t6 ON t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB t7 ON t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY t8 ON t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY t9 ON t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY t10 ON t1.ZRYYDMF_ID = t10.ID
        |	LEFT JOIN DM_XHGG b5 on b5.ID = b1.XHGG_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1') b6 on b6.ID = b1.ZZDW_ID
        |	LEFT JOIN DM_SSXS b7 on b7.ID = b1.SBXS_ID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1' and b1.data_type = '2'
        |""".stripMargin)
  }

  def sql4(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        | '4' SBLX,
        | case
        |   when b1.BDZ_ID is not null then nvl(t4.WZDWBM,b1.BDZ_ID)
        |   when  b1.XSDW_ID is not null then nvl(t2.WZDWBM,b1.XSDW_ID)
        |   when   b1.JC_ID is not null then nvl(t3.WZDWBM,b1.JC_ID)
        | end as wzdwbm,
        |	b1.AZWZDM,
        |	b1.AZWZMC,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.CCBH,
        |	to_unix_timestamp(b1.CCRQ) CCRQ,
        |	b1.DDDW,
        |	b1.DYDJ,
        |	b1.ID,
        |	b1.JC_ID,
        |	b1.PDBJ,
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.TZ,
        |	b1.XSDW_ID,
        |	b1.YZ,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        |	b1.RERUN_FLAG,
        |	b1.BDZ_ID,
        |	b1.ZCSX_ID,
        |	b1.DDDW_ID,
        |	b1.SZDW_ID,
        |	b1.XHGG_ID,
        |	b1.ZZDW_ID SJZZDWID ,
        |	b1.SBXS_ID,
        |	b1.data_type,
        |	b5.XHGGDM XHGG,
        |	b6.QYMC SJZZDWMC,
        | b6.QYDM SJZZDWLX,
        |	b7.SSXSMC SBXS,
        |	b1.SBLY,
        |	t1.ID AS YXID,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        |   CASE
        |    	  WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		    ELSE t1.ZYCXSJ
        |   END AS ZYCXSJ,
        |   CASE
        |		    WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		    WHEN t1.ZTFL = 'LO' THEN NULL
        |	     	ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	  END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	t4.DWMC AS BDZMC,
        |	t4.BJDWBM AS BDZDM,
        |	t1.ZC_ID,
        |	t5.AZWZDM AS WZXLDM,
        |	t5.AZWZMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |	end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC
        |    ,e.empty as ZHDQJGID
        |    ,e.empty as ZHDQID
        |	   ,e.empty DLGS
        |    ,e.empty YJLX
        |    ,e.empty DLZDT
        |    ,e.empty DLZJT
        |    ,e.empty DTCCRQ
        |    ,e.empty DTZZC
        |    ,e.empty DTZZC_ID
        |    ,e.empty EDDL
        |    ,e.empty JCRQ
        |    ,e.empty JXFS
        |    ,e.empty JYCCRQ
        |    ,e.empty JYCLJS
        |    ,e.empty JYZZC
        |    ,e.empty JYZZC_ID
        |    ,e.empty MXXS
        |    ,e.empty MXXS_ID
        |    ,e.empty QYBM
        |    ,e.empty QYJB
        |    ,e.empty SGDW
        |    ,e.empty SGDW_ID
        |    ,b1.EDRL XLCD
        |    ,e.empty XLDM
        |    ,e.empty XLFDS
        |    ,e.empty XLLB
        |    ,e.empty XLMC
        |    ,e.empty XLXZ
        |    ,e.empty XL_ID
        |    ,e.empty YXZDDL
        |    ,e.empty ZCD
        |    ,e.empty ZSPL
        |    ,e.empty ZHSBLX
        |    ,e.empty ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |    ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM
        |	ZC_DLHGQ b1
        |	LEFT JOIN YX_DLHGQ t1 ON b1.id = t1.zc_id
        |	      AND t1.del_status = '1'
        |       and t1.data_type = '2'
        |       and b1.JC_ID=t1.JC_ID
        |       and b1.XSDW_ID=t1.XSDW_ID
        |       and b1.BDZ_ID=t1.BDZDM_ID
        |       and b1.dydj=t1.dydj
        |	LEFT JOIN DM_XSDW t2 ON b1.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC t3 ON b1.JC_ID = t3.ID
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN DM_BDZ t4 ON b1.BDZ_ID = t4.ID
        |	LEFT JOIN ZC_DLHGQ t5 ON t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ t6 ON t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB t7 ON t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY t8 ON t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY t9 ON t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY t10 ON t1.ZRYYDMF_ID = t10.ID
        |	LEFT JOIN DM_XHGG b5 on b5.ID = b1.XHGG_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1') b6 on b6.ID = b1.ZZDW_ID
        |	LEFT JOIN DM_SSXS b7 on b7.ID = b1.SBXS_ID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1' and b1.data_type = '2'
        |""".stripMargin)
  }

  def sql3(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |    '3' SBLX,
        |	case when b1.BDZ_ID is not null then nvl(t4.WZDWBM,b1.BDZ_ID)
        |      when b1.XSDW_ID is not null then nvl(t2.WZDWBM,b1.XSDW_ID)
        |      when b1.JC_ID is not null then nvl(t3.WZDWBM,b1.JC_ID)
        |      end as wzdwbm,
        |	b1.AZWZDM,
        |	b1.AZWZMC,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.CCBH,
        |	to_unix_timestamp(b1.CCRQ) CCRQ,
        |	b1.DDDW,
        |	b1.DYDJ,
        |	b1.EDDL,
        |	b1.ID,
        |	b1.JC_ID,
        |	b1.PDBJ,
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.TZ,
        |	b1.XSDW_ID,
        |	b1.YZ,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        |	b1.RERUN_FLAG,
        |	b1.BDZ_ID,
        |	b1.ZCSX_ID,
        |	b1.DDDW_ID,
        |	b1.SZDW_ID,
        |	b1.XHGG_ID,
        |	b1.ZZDW_ID SJZZDWID,
        |	b1.SBXS_ID,
        |	b1.data_type,
        |	b5.XHGGDM XHGG,
        |	b6.QYMC SJZZDWMC,
        | b6.QYDM SJZZDWLX,
        |	b7.SSXSMC SBXS,
        |	b1.SBLY,
        |	t1.ID AS YXID,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        |    CASE
        |    	WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		ELSE t1.ZYCXSJ
        |    END AS ZYCXSJ,
        |    CASE
        |		WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		WHEN t1.ZTFL = 'LO' THEN NULL
        |		ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	t4.DWMC AS BDZMC,
        |	t4.BJDWBM AS BDZDM,
        |	t1.ZC_ID,
        |	t5.AZWZDM AS WZXLDM,
        |	t5.AZWZMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |		end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC
        |    ,e.empty as ZHDQJGID
        |    ,e.empty as ZHDQID
        |	   ,e.empty DLGS
        |    ,e.empty DLZDT
        |    ,e.empty DLZJT
        |    ,e.empty DTCCRQ
        |    ,e.empty DTZZC
        |    ,e.empty DTZZC_ID
        |    ,e.empty JCRQ
        |    ,e.empty JXFS
        |    ,e.empty JYCCRQ
        |    ,e.empty YJLX
        |    ,e.empty JYCLJS
        |    ,e.empty JYZZC
        |    ,e.empty JYZZC_ID
        |    ,e.empty MXXS
        |    ,e.empty MXXS_ID
        |    ,e.empty QYBM
        |    ,e.empty QYJB
        |    ,e.empty SGDW
        |    ,e.empty SGDW_ID
        |    ,b1.DDRL XLCD
        |    ,e.empty XLDM
        |    ,e.empty XLFDS
        |    ,e.empty XLLB
        |    ,e.empty XLMC
        |    ,e.empty XLXZ
        |    ,e.empty XL_ID
        |    ,e.empty YXZDDL
        |    ,e.empty ZCD
        |    ,e.empty ZSPL
        |    ,e.empty ZHSBLX
        |    ,e.empty ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |    ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM
        |	ZC_DLQ b1
        |	LEFT JOIN YX_DLQ t1 ON b1.id = t1.zc_id
        |       and t1.del_status='1'
        |       and t1.data_type = '2'
        |       and b1.JC_ID=t1.JC_ID
        |       and b1.XSDW_ID=t1.XSDW_ID
        |       and b1.BDZ_ID=t1.BDZDM_ID
        |       and b1.dydj=t1.dydj
        |	LEFT JOIN DM_XSDW t2 ON b1.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC t3 ON b1.JC_ID = t3.ID
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        | left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN DM_BDZ t4 ON b1.BDZ_ID = t4.ID
        |	LEFT JOIN ZC_DLQ t5 ON t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ t6 ON t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB t7 ON t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY t8 ON t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY t9 ON t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY t10 ON t1.ZRYYDMF_ID = t10.ID
        |	LEFT JOIN DM_XHGG b5 ON b5.ID = b1.XHGG_ID
        |	LEFT JOIN (select * from DM_QY where del_status = '1') b6 ON b6.ID = b1.ZZDW_ID
        |	LEFT JOIN DM_SSXS b7 ON b7.ID = b1.SBXS_ID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1' and b1.data_type = '2'
        |""".stripMargin)
  }

  def sql2(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |    '0' SBLX,
        |	case
        |    when  b1.XSDW_ID is not null then nvl(t2.WZDWBM,b1.XSDW_ID)
        |    when  b1.JC_ID is not null then nvl(t3.WZDWBM,b1.JC_ID)
        |    end as wzdwbm,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.DDDW,
        |	b1.QYBM,
        |	b1.DYDJ,
        |	b1.ID,
        |	to_unix_timestamp(b1.JCRQ) JCRQ,
        |	b1.JC_ID,
        |	b1.QYJB,
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.XLCD,
        |	b1.XLFDS,
        |	b1.XLLB,
        |	b1.RERUN_FLAG,
        |	b1.XLXZ,
        |	b1.XSDW_ID,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        | concat (b1.XLSSDW,b1.XLDM )	XL_ID,
        |	b1.zcsx_id,
        |	b1.dddw_id,
        |	b1.szdw_id,
        |	b1.data_type,
        |	b4.QYMC AS SGDW,
        | b4.ID SGDW_ID,
        |	b1.XLDM,
        |	b1.XLMC,
        |	b1.SBLY,
        |	t1.ID AS YXID,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ,
        |	t1.ZYQBYSJ,
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,
        |	t1.ZYHBYSJ,
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        |    CASE
        |    	WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN NULL
        |		ELSE t1.ZYCXSJ
        |    END AS ZYCXSJ,
        |    CASE
        |		WHEN t1.ZTFL IN ( 'DR' ) THEN t1.ZYCXSJ
        |		WHEN t1.ZTFL = 'LO' THEN NULL
        |		ELSE ROUND((to_unix_timestamp(t1.ZZSJ)-to_unix_timestamp(t1.QSSJ))/60/60,2)
        |	END AS CXSJ,
        |	t3.DWMC AS JCMC,
        |	t3.BJDWBM AS JCDM,
        |	t2.DWMC AS XSDWMC,
        |	t2.BJDWBM AS XSDWDM,
        |	e.empty AS BDZMC,
        |	e.empty AS BDZDM,
        |	t1.ZC_ID,
        |	t5.XLDM AS WZXLDM,
        |	t5.XLMC AS WZXLMC,
        |	t1.ZTFL,
        |	t6.SJZTMC AS ZTFLMC,
        |	t6.SJZTBM AS ZTFLBM,
        |	concat (t7.PARENT_ID,t7.SBDM ) AS TDSBDM,
        |	case
        |		when t7.ZSBLBMC is NULL then NULL
        |		else concat_ws(',',t7.ZSBLBMC,t7.XTHSBMC,t7.ZXTHBJMC,t7.SBMC )
        |		end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t8.JSYYDM,
        |	t8.JSYYMC,
        |	t1.ZRYYDM_ID,
        |	concat(t10.ZRYYDM,t9.ZRYYDM ) AS ZRYYDM,
        |	concat_ws(' ',t10.ZRYYMC,t9.ZRYYMC ) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t10.ZRYYMC AS ZRYYFMC
        |    ,e.empty as ZHDQJGID
        |    ,e.empty as ZHDQID
        |	   ,b1.XLDM as AZWZDM
        |    ,b1.XLMC AZWZMC
        |    ,e.empty BDZ_ID
        |    ,e.empty CCBH
        |    ,e.empty CCRQ
        |    ,e.empty DLGS
        |    ,e.empty DLZDT
        |    ,e.empty DLZJT
        |    ,e.empty DTCCRQ
        |    ,e.empty YJLX
        |    ,e.empty DTZZC
        |    ,e.empty DTZZC_ID
        |    ,e.empty EDDL
        |    ,e.empty JXFS
        |    ,e.empty JYCCRQ
        |    ,e.empty JYCLJS
        |    ,e.empty JYZZC
        |    ,e.empty JYZZC_ID
        |    ,e.empty MXXS
        |    ,e.empty MXXS_ID
        |    ,e.empty PDBJ
        |    ,e.empty SBXS
        |    ,e.empty SBXS_ID
        |    ,e.empty TZ
        |    ,e.empty XHGG
        |    ,e.empty XHGG_ID
        |    ,e.empty YXZDDL
        |    ,e.empty YZ
        |    ,e.empty ZCD
        |    ,e.empty ZSPL
        |    ,b4.QYMC SJZZDWMC
        |    ,b4.QYDM SJZZDWLX
        |    ,b1.SJDW_ID SJZZDWID
        |    ,e.empty ZHSBLX
        |    ,t1.ismerge ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |    ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM
        |	ZC_JKXL b1
        |	LEFT JOIN yx_jkxl t1 ON b1.id = t1.ZC_ID
        |    and t1.del_status='1'
        |    and t1.data_type = '2'
        |    and b1.JC_ID=t1.JC_ID
        |    and b1.XSDW_ID=t1.XSDW_ID
        |    and b1.dydj=t1.dydj
        |	LEFT JOIN DM_XSDW t2  ON b1.XSDW_ID = t2.ID
        |	LEFT JOIN DM_JC   t3  ON b1.JC_ID = t3.ID
        |    left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        |    left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  t3.WZDWBM like CONCAT(d2.WZDWBM,'%')
        |    left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  t3.WZDWBM like CONCAT(d3.WZDWBM,'%')
        |    left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  t3.WZDWBM like CONCAT(d4.WZDWBM,'%')
        |    left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  t3.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |	LEFT JOIN ZC_JKXL t5  ON t1.ZC_ID = t5.ID
        |	LEFT JOIN DM_SJ   t6  ON t1.ZTFL = t6.SJZTFH
        |	LEFT JOIN DM_SB   t7  ON t1.TDSBDM_ID = t7.ID
        |	LEFT JOIN DM_JSYY t8  ON t1.JSYYDM_ID = t8.ID
        |	LEFT JOIN DM_ZRYY t9  ON t1.ZRYYDM_ID = t9.ID
        |	LEFT JOIN DM_ZRYY t10 ON t1.ZRYYDMF_ID = t10.ID
        | LEFT JOIN (select * from DM_QY where del_status = '1')   b4  ON b4.ID = b1.SJDW_ID
        |	LEFT JOIN EMPTY e on 1=1
        | where b1.del_status = '1' and b1.data_type = '2'
        |""".stripMargin)
  }

  def sql1(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |   '1' SBLX,
        |    case
        |       when b1.BDZ_ID is not null then nvl(b4.WZDWBM,b1.BDZ_ID)
        |       when b1.XSDW_ID is not null then nvl(b3.WZDWBM,b1.XSDW_ID)
        |       when b1.JC_ID is not null then nvl(b2.WZDWBM,b1.JC_ID)
        |    end as wzdwbm,
        |	b1.AZWZDM,
        |	b1.AZWZMC,
        |	b1.BDFL,
        |	b1.BDH,
        |	b1.BDLX,
        |	to_unix_timestamp(b1.BDRQ) BDRQ,
        |	b1.BDYY,
        |	b1.CCBH,
        |	to_unix_timestamp(b1.CCRQ) CCRQ,
        |	b1.DDDW,
        |	b1.DYDJ,--电压等级
        |	b1.ID,
        |	b1.JC_ID,--单位级别(局厂)
        |	b1.QYLX,
        |	b1.REMARK,
        |	b1.SBXS,
        |	b1.SB_ID,
        |	b1.SDBZ,
        |	b1.SZDW,
        |	to_unix_timestamp(case when b1.CHANGETCRQ is null then b1.TCRQ else b1.CHANGETCRQ end) TCRQ,
        |	to_unix_timestamp(b1.TOUYUNRQ) TOUYUNRQ,
        |	to_unix_timestamp(case when b1.CHANGETUIYIRQ is null then b1.TUIYIRQ else b1.CHANGETUIYIRQ end) TUIYIRQ,
        |	b1.TZ,
        |	nvl(b1.XSDW_ID,"") XSDW_ID,--单位级别(下属单位)
        |	b1.YZ,
        |	to_unix_timestamp(case when b1.CHANGEZCRQ is null then b1.ZCRQ else b1.CHANGEZCRQ end) ZCRQ,
        |	b1.ZCSX,
        |	b1.RERUN_FLAG,
        |	nvl(b1.BDZ_ID,"") BDZ_ID,--单位级别(变电站)
        |	b1.ZCSX_ID,
        |	b1.DDDW_ID,
        |	b1.SZDW_ID,
        |	b1.XHGG_ID,
        |	nvl(b1.ZZDW_ID,"") SJZZDWID, --制造单位id
        |	b1.SBXS_ID,
        |	b1.data_type,
        |	b2.DWMC AS JCMC,
        |	b2.BJDWBM  AS JCDM,
        |	b3.BJDWBM AS XSDWDM,
        |	b3.DWMC XSDWMC,
        |	b4.BJDWBM AS BDZDM,
        |	b4.DWMC AS BDZMC,
        |	b5.XHGGDM AS XHGG,
        |	b6.QYMC SJZZDWMC,
        | b6.QYDM SJZZDWLX,
        |	t1.ID AS YXID,
        |	to_unix_timestamp(t1.QSSJ) QSSJ,--运行事件起始时间
        |	t1.KY1,
        |	t1.KY2,
        |	to_unix_timestamp(t1.ZYQSSJ) ZYQSSJ, --运行事件作业起始时间
        |	t1.ZYQBYSJ,-- 作业前备用时间
        |	to_unix_timestamp(t1.ZYZZSJ) ZYZZSJ,-- 运行事件作业终止时间
        |	to_unix_timestamp(t1.ZZSJ) ZZSJ,-- 运行事件终止时间
        |	t1.ZYHBYSJ, --作业后备用时间
        |	t1.TQZK,
        |	t1.TSYY,
        |	t1.JHTYFL,
        |	t1.JHYQ,
        |	t1.SBGH,
        |	t1.RWH,
        |	t1.RWSM,
        |	t1.CYTJ,
        |	t1.EVENT_ID,
        |	t1.SSLX,
        |    CASE
        |    		WHEN t1.ZTFL IN ( 'DR', 'PR' ) THEN 0.0
        |       ELSE t1.ZYCXSJ
        |    END AS ZYCXSJ,
        |    CASE
        |		WHEN t1.ZTFL = 'LO' THEN T1.zycxsj
        |    ELSE ROUND( ( cast(t1.ZZSJ as decimal) - cast(t1.QSSJ as decimal) ) * 24, 2 )
        |	END AS CXSJ,
        |	t1.ZC_ID,
        |	t1.ZTFL,
        |    t2.SJZTMC AS ZTFLMC,
        |	t2.SJZTBM AS ZTFLBM,
        |	concat(t3.PARENT_ID,t3.SBDM) AS TDSBDM,
        |	case
        |	  when t3.ZSBLBMC = null then null
        |	  else  concat_ws(',',t3.ZSBLBMC,t3.XTHSBMC,t3.ZXTHBJMC,t3.SBMC)
        |	  end AS TDSBMC,
        |	t1.JSYYDM_ID,
        |	t4.JSYYDM AS JSYYDM,
        |	t4.JSYYMC AS JSYYMC,
        |	t1.ZRYYDM_ID,
        |    concat(t5.ZRYYDM,t6.ZRYYDM) AS ZRYYDM,
        |	concat(t5.ZRYYMC,t6.ZRYYMC) AS ZRYYMC,
        |	t1.ZRYYDMF_ID,
        |	t5.ZRYYMC AS ZRYYFMC
        |    ,e.empty as ZHDQJGID
        |    ,e.empty as ZHDQID
        |    ,e.EMPTY AS DLZDT
        |    ,e.EMPTY AS DLZJT
        |    ,e.EMPTY AS DTCCRQ
        |    ,e.EMPTY AS DTZZC
        |    ,e.EMPTY AS DTZZC_ID
        |    ,e.EMPTY AS EDDL
        |    ,e.EMPTY AS JCRQ
        |    ,e.EMPTY AS JXFS
        |    ,e.EMPTY AS JYCCRQ
        |    ,e.EMPTY AS JYCLJS
        |    ,e.EMPTY AS JYZZC
        |    ,e.EMPTY AS JYZZC_ID
        |    ,e.EMPTY AS MXXS
        |    ,e.EMPTY AS MXXS_ID
        |    ,e.EMPTY AS PDBJ
        |    ,e.EMPTY AS QYBM
        |    ,e.EMPTY AS QYJB
        |    ,e.EMPTY AS SBLY
        |    ,e.empty YJLX
        |    ,e.EMPTY AS SGDW
        |    ,e.empty SGDW_ID
        |    ,e.EMPTY AS WZXLDM
        |    ,e.EMPTY AS WZXLMC
        |    ,b1.RL AS XLCD
        |    ,e.EMPTY AS XLDM
        |    ,e.EMPTY AS XLFDS
        |    ,e.EMPTY AS XLLB
        |    ,e.EMPTY AS XLMC
        |    ,e.EMPTY AS XLXZ
        |    ,e.EMPTY AS XL_ID
        |    ,e.EMPTY AS YXZDDL
        |    ,e.EMPTY AS ZCD
        |    ,e.EMPTY AS ZSPL
        |    ,e.empty ZHSBLX
        |    ,e.empty ISMERGE
        |    ,e.empty JGZCRQ
        |    ,e.empty JGTCRQ
        |    ,e.empty JGTUIYIRQ
        |    ,t1.TDSBDMF_ID
        |    ,t1.TDSBDMS_ID
        |    ,t1.TDSBDMT_ID
        |    ,t1.TDSBDM_ID
        |    ,d1.DWMC DW1MC
        |    ,d2.DWMC DW2MC
        |    ,d3.DWMC DW3MC
        |    ,d4.DWMC DW4MC
        |    ,d5.DWMC DW5MC
        |    ,e.empty JG_AZWZDM
        |    ,e.empty JG_AZWZMC
        |    ,e.empty ZHDQ_AZWZDM
        |    ,e.empty ZHDQ_AZWZMC
        |    ,e.empty ZHDQZCRQ
        |    ,e.empty ZHDQTCRQ
        |    ,e.empty ZHDQTUIYIRQ
        |FROM
        |	ZC_BYQ b1
        |	LEFT JOIN yx_byq t1 ON b1.id = t1.zc_id
        |    and t1.del_status='1'
        |    and t1.data_type = '2'
        |    and b1.JC_ID=t1.JC_ID
        |    and b1.XSDW_ID=t1.XSDW_ID
        |    and b1.BDZ_ID=t1.BDZDM_ID
        |    and b1.dydj=t1.dydj
        |    left join DM_JC     b2  on  b1.JC_ID = b2.ID
        |    left join (select DWMC,WZDWBM from DM_LSGX where DWJB='1' and del_status = '1') d1  on  1=1
        |    left join (select DWMC,WZDWBM from DM_LSGX where DWJB='2' and del_status = '1') d2  on  b2.WZDWBM like CONCAT(d2.WZDWBM,'%')
        |    left join (select DWMC,WZDWBM from DM_LSGX where DWJB='3' and del_status = '1') d3  on  b2.WZDWBM like CONCAT(d3.WZDWBM,'%')
        |    left join (select DWMC,WZDWBM from DM_LSGX where DWJB='4' and del_status = '1') d4  on  b2.WZDWBM like CONCAT(d4.WZDWBM,'%')
        |    left join (select DWMC,WZDWBM from DM_LSGX where DWJB='5' and del_status = '1') d5  on  b2.WZDWBM like CONCAT(d5.WZDWBM,'%')
        |    left join DM_XSDW   b3  on  b3.ID = b1.XSDW_ID
        |    left join DM_BDZ    b4  on  b4.ID = b1.BDZ_ID
        |    left join DM_XHGG   b5  on  b5.ID = b1.XHGG_ID
        |    left join (select * from DM_QY where del_status = '1')   b6  on  b6.ID = b1.ZZDW_ID
        |    left join DM_SJ     t2  on  t1.ZTFL = t2.SJZTFH
        |    left join DM_SB     t3  on  t1.TDSBDM_ID = t3.ID
        |    left join DM_JSYY   t4  on  t1.JSYYDM_ID = t4.ID
        |    left join DM_ZRYY   t5  on  t1.ZRYYDMF_ID = t5.ID
        |    left join DM_ZRYY   t6  on  t1.ZRYYDM_ID = t6.ID
        |    left join EMPTY      e on 1 = 1
        |    where b1.del_status = '1' and b1.data_type = '2'
        |""".stripMargin)

  }
}
