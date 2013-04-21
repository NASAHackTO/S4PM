#!/bin/ksh

#
# name: gdaac_ProcIncrementEcAcRequestId.sql
# purpose: create a gdaac-specific stored proc that will return a
#          request Id for use with DCLI
# revised: 06/20/2003 yh, lhf creation
# notes: SYBASE, sybase_login, sybase_password, dbname, DSQUERY env
#        variables must be set first, also the name was changed to
#        gd_ because gdaac_ put it over the 30 character limit
#
################################################################################
# gd_ProcIncrementEcAcRequestId.ksh,v 1.2 2006/09/12 20:31:38 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

$SYBASE/bin/isql -U$sybase_login -P$sybase_passwd << EOF 


USE $dbname
go

if exists (select 1
            from  sysobjects
           where  name = 'gd_ProcIncrementEcAcRequestId'
            and   type = 'P' and
            user_name(uid) = "dbo")
  BEGIN
    drop procedure gd_ProcIncrementEcAcRequestId
  END
go

CREATE PROCEDURE gd_ProcIncrementEcAcRequestId
                 (@RequestIdString varchar(10) output)
AS
BEGIN
--       BEGIN TRAN

       DECLARE @rcount int,
               @id_length int,
               @site_id varchar(2),
               @leading_zeros int,
               @num_zeros varchar(10),
               @request_id varchar(10),
               @request_string varchar(10),
               @requestId NUMERIC(10,0)

--              SELECT @rcount = count(*) FROM EcAcRequestId
--       IF (@rcount > 1)
--       BEGIN
--      	  	RAISERROR 93000 
--      		ROLLBACK TRAN
--      		RETURN -999
--        END
--        IF (@rcount = 0)

        IF NOT EXISTS( SELECT 1 FROM EcAcRequestId) 
        BEGIN
                  BEGIN TRAN
                  INSERT INTO EcAcRequestId VALUES (1)
        END
        ELSE
        BEGIN 
                 BEGIN TRAN
	         UPDATE EcAcRequestId
	            SET requestId = requestId + 1

		IF @@error != 0
		BEGIN
			RAISERROR 93000 
			ROLLBACK TRAN
			RETURN -999
		END
         END
		
	       SELECT @requestId = requestId FROM  EcAcRequestId
       
       COMMIT TRAN

       SELECT @rcount = count(*) FROM EcMsDAACSites
       IF (@rcount = 0)
       BEGIN
                RAISERROR 93010 "ProcIncrementEcAcRequestId fail:  EcMsDAACSites table is empty"
                ROLLBACK TRAN
                RETURN -999
        END
        SELECT @site_id = DAAC_Id from EcMsDAACSites where This_DAAC = "T"
        SELECT @request_string = convert(varchar(10),@requestId)

        SELECT @request_id = convert(varchar(10),@requestId)
        SELECT @id_length = datalength(@request_id)
        SELECT @leading_zeros = 8 - @id_length
        SELECT @num_zeros = replicate ('0',@leading_zeros)
        SELECT @RequestIdString = @site_id+@num_zeros+@request_string

 
	SELECT @RequestIdString
        IF @@error != 0
        BEGIN
                RAISERROR 93000
--		ROLLBACK TRAN
                RETURN -999
        END


--	COMMIT TRAN
END
go
grant execute on gd_ProcIncrementEcAcRequestId to AcctGroup
go
grant execute on gd_ProcIncrementEcAcRequestId to anonymous
go
select name,crdate from sysobjects where name = 'gd_ProcIncrementEcAcRequestId'
go
sp_helprotect gd_ProcIncrementEcAcRequestId
go
EOF
