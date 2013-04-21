#!/bin/ksh 

#
# name: gdaac_ProcIncrementOrderId.sql
# purpose: create a gdaac-specific stored proc that will return a
#          request Id for use with DCLI
# revised: 06/20/2003 yh, lhf creation
# notes: SYBASE, sybase_login, sybase_password, dbname, DSQUERY env
#        variables must be set first
#
################################################################################
# gd_ProcIncrementOrderId.ksh,v 1.2 2006/09/12 20:31:38 sberrick Exp
# -@@@ S4PM, Version Release-5_27_0
################################################################################

$SYBASE/bin/isql -U$sybase_login -P$sybase_passwd << EOF 


use $dbname
go

if exists (select 1
            from  sysobjects
           where  name = 'gd_ProcIncrementOrderId'
            and   type = 'P' and
            user_name(uid) = "dbo")
  BEGIN
    drop procedure gd_ProcIncrementOrderId
  END
go


CREATE PROCEDURE gd_ProcIncrementOrderId
                 (@OrderIdString varchar(10) output)
AS
BEGIN
--       BEGIN TRAN
       DECLARE @id_length int
       DECLARE @site_id varchar(2)
       DECLARE @leading_zeros int
       DECLARE @num_zeros varchar(10)
       DECLARE @rcount int
       DECLARE @order_id varchar(10)
       DECLARE @order_string varchar(10)
       DECLARE @orderId NUMERIC(10,0)
--       SELECT @rcount = count(*) FROM EcAcOrderId
--       IF (@rcount > 1)
--       BEGIN
--                RAISERROR 93001
--                ROLLBACK TRAN
--                RETURN -999
--        END
--        IF (@rcount = 0)
--                INSERT INTO EcAcOrderId VALUES (0)
       BEGIN TRAN
       
	UPDATE EcAcOrderId
	   SET orderId = orderId + 1

        IF @@error != 0
        BEGIN
                RAISERROR 93001 
                ROLLBACK TRAN
                RETURN -999
        END
        
       SELECT @orderId = orderId FROM  EcAcOrderId
       
       COMMIT TRAN

       SELECT @rcount = count(*) FROM  EcMsDAACSites
       IF (@rcount = 0)
       BEGIN
                RAISERROR 93008 "ProcIncrementOrderId fail:  EcMsDAACSites table is empty"
                ROLLBACK TRAN
                RETURN -999
        END
        SELECT @site_id = DAAC_Id from EcMsDAACSites where  This_DAAC = "T"
        SELECT @order_string = convert(varchar(10),@orderId)

        SELECT @order_id = convert(varchar(10),@orderId)
        SELECT @id_length = datalength(@order_id)
        SELECT @leading_zeros = 8 - @id_length
        SELECT @num_zeros = replicate ('0',@leading_zeros)
        SELECT @OrderIdString = @site_id+@num_zeros+@order_string

        SELECT @OrderIdString

        IF @@error != 0
       BEGIN
                RAISERROR 93001 
--                ROLLBACK TRAN
                RETURN -999
        END

--	COMMIT TRAN
END
go
grant execute on gd_ProcIncrementOrderId to AcctGroup
go
grant execute on gd_ProcIncrementOrderId to anonymous
go
select name,crdate from sysobjects where name='gd_ProcIncrementOrderId'
go
sp_helprotect gd_ProcIncrementOrderId
go
EOF
