using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Text;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace extract_csv
{
    public class Function
    {
        IAmazonS3 S3Client { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        //public async Task<string> FunctionHandler(S3Event events, ILambdaContext context)
        public async Task FunctionHandler(S3Event events, ILambdaContext context)
        {
            foreach (var evnt in events.Records)
            {
                var s3Event = evnt.S3;
                if (s3Event == null)
                {
                    return;
                }

                context.Logger.LogLine($"Received notification of {evnt.EventName} for s3://{s3Event.Bucket.Name}/{s3Event.Object.Key}");

                // create some random temperature forecasts
                StringBuilder bld = new StringBuilder();
                Random r = new Random();
                for (int i = 0; i < 10; i++)
                {
                    int hour = r.Next(0, 24);
                    int tempInt = r.Next(0, 30);
                    int tempDec = r.Next(0, 9);
                    bld.AppendLine($"2020-09-28 {(hour < 10 ? "0" + hour.ToString() : hour.ToString())}:00:00,{tempInt}.{tempDec}");
                }

                // change the file extension to .csv from .nc
                string newKey = s3Event.Object.Key.Replace(".nc", ".csv");
                context.Logger.LogLine($"Going to write to file s3://bigwind-curated/{newKey} content: {bld}");

                var response = await S3Client.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest() { BucketName = "bigwind-curated", Key = newKey, ContentBody = bld.ToString(), ContentType = "text/plain" });
            }

            return;
        }

        /* Sample JSON document to use when testing this Lambda function
{
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws:s3",
      "awsRegion": "{region}",
      "eventTime": "1970-01-01T00:00:00Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "EXAMPLE123456789",
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "bigwind-ingest",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          },
          "arn": "arn:{partition}:s3:::mybucket"
        },
        "object": {
          "key": "78711792d75acbe108710d159b2eb582209562b1.nc",
          "size": 1024,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}
*/
    }
}
