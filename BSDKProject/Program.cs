using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace BSDKProject
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Service started...");
            bool isInserted = await FetchAndInsertData();
            if (isInserted)
            {
                Console.WriteLine("Data inserted successfully.");
            }
            else
            {
                Console.WriteLine("Data insertion failed.");
            }

            Console.ReadLine();
        }

        //static async Task InsertDataPeriodically()
        //{
        //    while (true)
        //    {
        //        Console.WriteLine($"Executing at: {DateTime.Now}");

        //        bool isInserted = await FetchAndInsertData();
        //        if (isInserted)
        //        {
        //            Console.WriteLine("Data inserted successfully.");
        //        }
        //        else
        //        {
        //            Console.WriteLine("Data insertion failed.");
        //        }

        //        await Task.Delay(TimeSpan.FromMinutes(2));
        //    }
        //}

        static async Task<bool> FetchAndInsertData()
        {
            string oracleConnectionString = "User Id=ORION;Password=ORION;Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.10.87)(PORT=1599)))(CONNECT_DATA=(SERVICE_NAME=ORION19)))";
            string sqlConnectionString = "Server=192.168.10.102;Database=FINLEDGER;User ID=sa;Password=cmc;Trusted_Connection=False;";

            string query = @"
          SELECT ACCOUNTID, 
       SUM(CASE WHEN DCFLAG = 'C' THEN Amount ELSE Amount * -1 END) AS AMOUNT,
       SYSDATE AS CURRENT_DATE,
       FDATE
FROM FINLEDGER
WHERE firm_id = '1001' 
  AND FDATE BETWEEN TO_DATE('2025-01-20', 'YYYY-MM-DD') AND TO_DATE('2025-03-03', 'YYYY-MM-DD')
  AND ACCOUNTID IN (SELECT AccountID FROM account_master WHERE Account_Type = 'CLIENT' AND firm_id = '1001')
GROUP BY ACCOUNTID, FDATE
HAVING SUM(CASE WHEN DCFLAG = 'C' THEN Amount ELSE Amount * -1 END) < 0
ORDER BY 2
";
            try
            {
                DataTable dt = new DataTable();

    
                using (OracleConnection conn = new OracleConnection(oracleConnectionString))
                {
                    await conn.OpenAsync();
                    using (OracleCommand cmd = new OracleCommand(query, conn))
                    {
                        using (OracleDataAdapter da = new OracleDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }
                    }
                }

                if (dt.Rows.Count > 0)
                {
                    using (SqlConnection sqlConn = new SqlConnection(sqlConnectionString))
                    {
                        await sqlConn.OpenAsync();

                        foreach (DataRow row in dt.Rows)
                        {
                            using (SqlCommand cmd = new SqlCommand("SP_INSERTFINLEDGER", sqlConn))
                            {
                                cmd.CommandType = CommandType.StoredProcedure;
                                cmd.Parameters.AddWithValue("@ACCOUNTID", row["ACCOUNTID"]);
                                cmd.Parameters.AddWithValue("@AMOUNT", row["AMOUNT"]);
                                cmd.Parameters.AddWithValue("@FDATE", row["FDATE"]);
                               
                                await cmd.ExecuteNonQueryAsync();
                            }
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] {DateTime.Now}: {ex.Message}");
                return false;
            }
        }
    }
}
