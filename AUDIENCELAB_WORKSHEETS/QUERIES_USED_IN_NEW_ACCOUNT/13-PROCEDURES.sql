CREATE OR REPLACE PROCEDURE FOUR_EYES.PUBLIC.DELETE_OLD_DATA()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var currentDate = new Date();
  var sevenDaysAgo = new Date();
  sevenDaysAgo.setDate(currentDate.getDate() - 7);

  var formattedDate = sevenDaysAgo.toISOString().split(''T'')[0];

  //return formattedDate;

  //var sql_command = `DELETE FROM FOUR_EYES.TEST.PREMADE_4EYES_TEST
   //             WHERE DATE < ''${formattedDate}''`

   //var sql_command = `SELECT COUNT(*) AS Row_Count FROM FOUR_EYES.PUBLIC.PREMADE_4EYES_LITE
    //            WHERE DATE < ''${formattedDate}''`;

   var sql_command = `DELETE FROM FOUR_EYES.PUBLIC.PREMADE_4EYES_LITE
                     WHERE DATE < ''${formattedDate}''`;

                     
  try {
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var rs = stmt.execute();
   return ''Old data successfully deleted.'';

  //var row;
  //while (rs.next()) {
   // row = rs.getColumnValue(1);
  //}
  //return row;
  } catch (err) {
    return ''Error deleting old data: '' + err.message;
  }
';