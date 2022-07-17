-- JK => ref - https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-db-index-physical-stats-transact-sql?view=sql-server-ver15#d-using-sysdm_db_index_physical_stats-in-a-script-to-rebuild-or-reorganize-indexes
-- Ensure a USE <databasename> statement has been executed first.  
SET NOCOUNT ON;  
DECLARE @objectid int;  
DECLARE @indexid int;  
DECLARE @partitioncount bigint;  
DECLARE @schemaname nvarchar(130);   
DECLARE @objectname nvarchar(130);   
DECLARE @indexname nvarchar(130);   
DECLARE @partitionnum bigint;  
DECLARE @partitions bigint;  
DECLARE @frag float;  
DECLARE @command nvarchar(4000);   
-- Conditionally select tables and indexes from the sys.dm_db_index_physical_stats function   
-- and convert object and index IDs to names.  
SELECT  
    object_id AS objectid,  
    index_id AS indexid,  
    partition_number AS partitionnum,  
    avg_fragmentation_in_percent AS frag  
INTO #work_to_do  
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL , NULL, 'LIMITED')  
WHERE avg_fragmentation_in_percent > 10.0 AND index_id > 0;  

-- Declare the cursor for the list of partitions to be processed.  
DECLARE partitions CURSOR FOR SELECT * FROM #work_to_do;  

-- Open the cursor.  
OPEN partitions;  

-- JK => Loop through the partitions. 
-- basically loops through all indexes 1 at a time  
-- which I believe if you stop it only rolls back the one you were working on 
WHILE (1=1)  
    BEGIN;  
        FETCH NEXT  
           FROM partitions  
           INTO @objectid, @indexid, @partitionnum, @frag;  
        IF @@FETCH_STATUS < 0 BREAK;  
        SELECT @objectname = QUOTENAME(o.name), @schemaname = QUOTENAME(s.name)  
        FROM sys.objects AS o  
        JOIN sys.schemas as s ON s.schema_id = o.schema_id  
        WHERE o.object_id = @objectid;  
        SELECT @indexname = QUOTENAME(name)  
        FROM sys.indexes  
        WHERE  object_id = @objectid AND index_id = @indexid;  
        SEL.ECT @partitioncount = count (*)  
        FROM sys.partitions  
        WHERE object_id = @objectid AND index_id = @indexid;  

-- JK => 30 is an arbitrary decision point at which to switch between reorganizing and rebuilding.  
-- turn this on and the REBUILD off if you want to compare CPU burden
-- REORGANIZE is very CPU noisy while REBUILD is more memory heavy and generally a better choice
-- maybe drop a higher number like @frag >= 20.0 
        -- IF @frag < 30.0  
        --     SET @command = N'ALTER INDEX ' + @indexname + N' ON ' + @schemaname + N'.' + @objectname + N' REORGANIZE';  
        IF @frag >= 20.0  -- JK => sets % frag level to run cleaner
            SET @command = N'ALTER INDEX ' + @indexname + N' ON ' + @schemaname + N'.' + @objectname + N' REBUILD';  
        IF @partitioncount > 1  
            SET @command = @command + N' PARTITION=' + CAST(@partitionnum AS nvarchar(10));  
        EXEC (@command);  
        PRINT N'Executed: ' + @command;  
    END;  

-- Close and deallocate the cursor.  
CLOSE partitions;  
DEALLOCATE partitions;  

-- Drop the temporary table.  
DROP TABLE #work_to_do;  
GO  

-- JK => it is important to update stats after indexes have changed
-- we have seen issues in Azure Sql where just updating stats somehow fixes it
EXEC sp_updatestats;