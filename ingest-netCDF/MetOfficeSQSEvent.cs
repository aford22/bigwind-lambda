using System.Collections.Generic;

namespace ingest_netCDF
{
    /// <summary>
    /// Subclass the AWS SDK to receive the non standard "Message" field instead of the "Body" field that the AWS SDK expects
    /// </summary>
    public class MetOfficeSQSEvent : Amazon.Lambda.SQSEvents.SQSEvent
    {
        public new List<MetOfficeSQSMessage> Records { get; set; }

        public class MetOfficeSQSMessage : SQSMessage
        {
            /// <summary>
            /// New field to receive the usual “Body” content 
            /// </summary>
            public string Message { get; set; }
        }
    }
}
