using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace D2CMessageHandler
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run([EventHubTrigger("", Connection = "IoTHubConnection")] EventData[] events, ILogger log)
        {
            string hum;
            string temp;
            float humValue;
            float tempValue;
            float lowerThreshold;
            float upperThreshold;

            foreach (EventData eventData in events)
            {
                string[] msg = eventData.EventBody.ToString().Split('"');
                string[] charsToDelete = new string[] {":"," ", ",", "}"};
                foreach (string c in charsToDelete)
                {
                    msg[2] = msg[2].Replace(c, string.Empty);
                    msg[4] = msg[4].Replace(c, string.Empty);
                    msg[6] = msg[6].Replace(c, string.Empty);
                    msg[8] = msg[8].Replace(c, string.Empty);
                }
                hum = msg[1];
                temp= msg[3];
                humValue = float.Parse(msg[2]);
                tempValue = float.Parse(msg[4]);
                lowerThreshold = float.Parse(msg[6]);
                upperThreshold = float.Parse(msg[8]);    

                string dbConn = Environment.GetEnvironmentVariable("SQLDbConnection");
                SqlConnection conn = new SqlConnection(dbConn);
                conn.Open();
                log.LogInformation(DateTime.Now.ToString());
                log.LogInformation(humValue.ToString());
                log.LogInformation(tempValue.ToString());
                log.LogInformation(lowerThreshold.ToString());
                log.LogInformation(upperThreshold.ToString());
                string text = $"INSERT INTO [dbo].[DHT11Readings] VALUES (GetDate(), {humValue}, {tempValue}, {upperThreshold}, {lowerThreshold})";
                SqlCommand cmd = new SqlCommand(text, conn);
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
}
