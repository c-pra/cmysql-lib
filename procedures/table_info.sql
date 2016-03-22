/*

table_info.sql (This file is part of cmysql_lib)


The MIT License (MIT)

Copyright (c) 2016 Christian Praxmarer

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

DELIMITER $$

DROP PROCEDURE IF EXISTS `get_table_info`$$
CREATE PROCEDURE `get_table_info` (
	IN dbname VARCHAR(64))
BEGIN
	DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN 
		-- Check passed data base name
		IF ((dbname IS NULL) OR (LENGTH(dbname) NOT BETWEEN 1 AND 64)) THEN
			SIGNAL SQLSTATE '45000'
				SET MESSAGE_TEXT = 'An invalid parameter was passed to the stored procedure.';
		END IF; 
		
		-- Check if the data base exists
		if (SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = dbname) = 0 then
			SIGNAL SQLSTATE '45000'
				SET MESSAGE_TEXT = 'The data base was not found.';	
		END IF;
	END;
    
    SET @tmptbl = "cmysql_lib_get_table_info_tmptbl";
    
	-- Load the results into a temporary table
	CALL load_table_info(dbname, @tmptbl);
		    
    -- Get table names and row count values from the temp table
	SET @sql_seltmptbl = CONCAT('SELECT `Name`, `Rows` FROM ', @tmptbl, ' ORDER BY `name`;'); 
	-- Execute the statement
	PREPARE            stmt_seltmptbl FROM @sql_seltmptbl;                                                                                                                                               
	EXECUTE            stmt_seltmptbl;
	DEALLOCATE PREPARE stmt_seltmptbl;
    
   	-- Drops the temporary table 
	SET @sql_drptmptbl = CONCAT('DROP TEMPORARY TABLE IF EXISTS ', @tmptbl, ';');
	-- Execute the statement
	PREPARE            stmt_drptmptbl FROM @sql_drptmptbl;                                                                                                                                               
	EXECUTE            stmt_drptmptbl;            
	DEALLOCATE PREPARE stmt_drptmptbl;
END$$ 
 
DROP PROCEDURE IF EXISTS `load_table_info`$$
CREATE PROCEDURE `load_table_info` (
	IN dbname     CHAR(64), 
	IN tmptbl CHAR(64))
BEGIN
	DECLARE tblname CHAR(64);
	DECLARE eoquery INT DEFAULT FALSE;  

	-- Get all tables from the data base
    DECLARE curInformationSchema CURSOR FOR                                                                                                                                                  
		SELECT 
			tbl.table_name                                                                                                                                                 
		FROM 
			information_schema.tables tbl                                                                                                                                    
		WHERE 
			tbl.table_schema = dbname AND tbl.table_type = 'BASE TABLE';
            
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET eoquery = TRUE;
    
    -- Error handling
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN 
		-- Check the passed data base name
		IF ((dbname IS NULL) OR (LENGTH(dbname) NOT BETWEEN 1 AND 64)) THEN
			SIGNAL SQLSTATE '45000'
				SET MESSAGE_TEXT = 'An invalid parameter was passed to the stored procedure.';
		END IF; 
		
		-- Check whether the data base exists
		if (SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = dbname) = 0 then
			SIGNAL SQLSTATE '45000'
				SET MESSAGE_TEXT = 'The database was not found.';	
		END IF;    
        
		-- Check the passed temporary table name
		IF ((tmptbl IS NULL) OR (LENGTH(tmptbl) NOT BETWEEN 1 AND 64)) THEN
			SIGNAL SQLSTATE '45000'
				SET MESSAGE_TEXT = 'Invalid parameter specified for temporary table name.';
		END IF;    
    END;
   
    -- Drops the temporary table before we start
    SET @sql_drptmptbl = CONCAT('DROP TEMPORARY TABLE IF EXISTS ', tmptbl, ';');
    -- Execute the statement
	PREPARE            stmt_drptmptbl FROM @sql_drptmptbl;                                                                                                                                               
	EXECUTE            stmt_drptmptbl;            
	DEALLOCATE PREPARE stmt_drptmptbl;
    
    -- Create the new temp table where the result set is inserted.
	SET @sql_crttmptbl = CONCAT('CREATE TEMPORARY TABLE ', tmptbl,   ' ', 
					            '(                                     ', 
						        '   `Name` VARCHAR(64) PRIMARY KEY,    ',
                                '   `Rows` INT                         ',
                                ');                                    ');
	
	PREPARE            stmt_crttmptbl FROM @sql_crttmptbl;                                                                                                                                               
	EXECUTE            stmt_crttmptbl;
	DEALLOCATE PREPARE stmt_crttmptbl;
        
	OPEN curInformationSchema;
	-- Open the cursor and iterate through tables of the schema. Query the row count 
    -- for each table and insert the value into the temporary table
    query_loop: LOOP                                                                                                                                                       
		FETCH curInformationSchema INTO tblname;                                                                                                                                          

		-- Check end of result set
		IF eoquery THEN                                                                                                                                           
			LEAVE query_loop;                                                                                                                                              
		END IF;                                                                                                                                                             

		-- Query row count and insert the value into the temporary table
		SET @sql_instmptbl = CONCAT('INSERT INTO ', tmptbl, ' (name, rows) SELECT ''', tblname, ''', COUNT(*) AS Count FROM ' , dbname, '.', tblname);
        -- Execute statement
		PREPARE            stmt_instmptbl FROM @sql_instmptbl;
		EXECUTE            stmt_instmptbl;
		DEALLOCATE PREPARE stmt_instmptbl;
	END LOOP;                                                                                                                                                               

	CLOSE curInformationSchema;           
END$$

DELIMITER ;
;
